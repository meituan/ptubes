package com.meituan.ptubes.sdk.netty;

import com.google.protobuf.InvalidProtocolBufferException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.constants.ConsumeConstants;
import com.meituan.ptubes.sdk.constants.NettyConstants;
import com.meituan.ptubes.sdk.model.DataRequest;
import com.meituan.ptubes.sdk.model.ServerInfo;
import com.meituan.ptubes.sdk.config.ReaderConnectionConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import com.meituan.ptubes.common.utils.JacksonUtil;
import com.meituan.ptubes.sdk.consumer.FetchThread;
import com.meituan.ptubes.sdk.consumer.FetchThreadState;
import com.meituan.ptubes.sdk.protocol.RdsPacket.RdsEnvelope;

public class NettyHttpRdsCdcReaderConnection extends AbstractNettyHttpConnection
    implements RdsCdcReaderConnection {

    private static final String PROTOCOL_VERSION = "/v1";
    private static final String SERVER_INFO_PATH = "/getServerInfo";
    private static final String SUBSCRIBE_PATH = "/subscribe";
    private static final String FETCH_MESSAGES_PATH = "/fetchMessages";

    private long inboundBytes = 0;
    private long outboundBytes = 0;

    private ServerInfo targetServer;
    private Bootstrap bootstrap;
    private Channel channel;
    private EventLoopGroup eventLoopGroup;

    private RdsCdcHTTPConnectionHandler channelHandler;
    private ChannelCloseListener channelCloseListener;
    private SubscribeResponseProcessor subscribeResponseProcessor;
    private FetchMessagesResponseProcessor fetchMessagesResponseProcessor;
    private FetchThread fetchThread;
    private FetchThreadState fetchThreadState;
    private final Logger log;

    public NettyHttpRdsCdcReaderConnection(
        ServerInfo targetServer,
        FetchThread fetchThread
    ) {
        super();
        this.log = LoggerFactory.getLoggerByTask(
            NettyHttpRdsCdcReaderConnection.class,
            fetchThread.getTaskName()
        );

        this.targetServer = targetServer;
        this.fetchThread = fetchThread;
        this.fetchThreadState = fetchThread.getFetchThreadState();
        this.channelCloseListener = new RdsCdcChannelCloseListener();
        this.subscribeResponseProcessor = new SubscribeResponseProcessor();
        this.fetchMessagesResponseProcessor = new FetchMessagesResponseProcessor();

        initNetworkConfig();
    }

    private void initNetworkConfig() {
        this.bootstrap = new Bootstrap();
        this.channelHandler = new RdsCdcHTTPConnectionHandler(fetchThread.getTaskName());
        this.channelHandler.setSendRequestResultListener(new RdsCdcSendRequestResultListener());
        this.channelHandler.setChannelDisconnectListener(new RdsCdcChannelDisconnectListener());

        this.eventLoopGroup = new NioEventLoopGroup();
        this.bootstrap.group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(
                ChannelOption.SO_KEEPALIVE,
                true
            )
            .option(
                ChannelOption.TCP_NODELAY,
                true
            )
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel channel) throws Exception {
                    channel.pipeline()
                        .addLast(new IdleStateHandler(
                            0,
                            0,
                            NettyConstants.CHANNEL_IDLE_TIMEOUT
                        ))
                        .addLast(new ChannelIdleTimeoutHandler(fetchThread.getTaskName()))
                        .addLast(new HttpClientCodec())
                        .addLast(new HttpObjectAggregator(NettyConstants.MAX_CONTENT_LENGTH))
                        .addLast(channelHandler);
                }
            });
    }

    @Override
    public ServerInfo connect() {
        this.setState(State.CONNECTING);

        try {
            if (log.isDebugEnabled()) {
                log.debug("Connecting to target server: " + targetServer);
            }
            this.channel = this.bootstrap.connect(targetServer.getAddress())
                .sync()
                .channel();
        } catch (Throwable ex) {
            this.onConnectFailure(ex);
        }

        this.onConnectSuccess();
        return targetServer;
    }

    @Override
    public void subscribe(ReaderConnectionConfig readerConnectionConfig) {
        if (this.getState() != State.CONNECTED) {
            log.warn("Connection is unavailable for subscribe now, need pick reader again.");
            fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
            fetchThread.enqueueMessage(fetchThreadState);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("TaskName " + fetchThread.getTaskName() + ", Subscribe Channel Id: " +
                          this.channel.id() +
                          ", subscribe info " + readerConnectionConfig.toString());
        }

        byte[] requestBody = JacksonUtil.toJson(
            readerConnectionConfig
        )
            .getBytes();
        ByteBuf content = Unpooled.wrappedBuffer(requestBody);
        FullHttpRequest fullHttpRequest = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.POST,
            getSuscribeURI(),
            content
        );

        fullHttpRequest.headers()
            .set(
                HttpHeaderNames.CONTENT_LENGTH,
                requestBody.length
            );

        try {
            ChannelPromise promise = this.channelHandler.sendMessage(
                fullHttpRequest,
                this.subscribeResponseProcessor
            );
            promise.await(ConsumeConstants.FETCH_REQUEST_TIMEOUT_MS);
        } catch (IllegalStateException e) {
            this.onSendRequestFailure(
                fullHttpRequest,
                e
            );
        } catch (InterruptedException ex) {
            log.error(
                "Subscribe info waiting for response error, might lead to fetch thread stuck, critical issue!!!",
                ex
            );
        }

        this.outboundBytes += requestBody.length;
    }

    @Override
    public void fetchMessages(
        DataRequest dataRequest,
        FetchThreadState fetchThreadState
    ) {
        if (this.getState() != State.CONNECTED) {
            log.warn("Connection is unavailable for connect now, need pick reader again.");
            fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
            fetchThread.enqueueMessage(fetchThreadState);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("TaskName " + fetchThread.getTaskName() + ", Fetch Channel Id: " +
                          this.channel.id());
        }
        byte[] requestBody = JacksonUtil.toJson(
            dataRequest
        )
            .getBytes();

        ByteBuf content = Unpooled.wrappedBuffer(requestBody);
        FullHttpRequest fullHttpRequest = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.POST,
            getFetchMessagesURI(),
            content
        );

        fullHttpRequest.headers()
            .set(
                HttpHeaderNames.CONTENT_LENGTH,
                requestBody.length
            );

        try {
            fetchThread.setLastFetchTime(System.currentTimeMillis());
            ChannelPromise promise = this.channelHandler.sendMessage(
                fullHttpRequest,
                this.fetchMessagesResponseProcessor
            );
            promise.await(ConsumeConstants.FETCH_REQUEST_TIMEOUT_MS);
        } catch (IllegalStateException e) {
            this.onSendRequestFailure(
                fullHttpRequest,
                e
            );
        } catch (InterruptedException ex) {
            log.error("Fetch message waiting for response error, might lead to fetch thread stuck, critical issue!!!");
        }

        this.outboundBytes += requestBody.length;
    }

    @Override
    public void close() {
        this.setState(State.CLOSING);

        if (channel != null) {
            ChannelFuture channelFuture = channel.close();
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        channelCloseListener.onChannelClose();
                    }
                }
            });
        }

        super.close();
        this.eventLoopGroup.shutdownGracefully();
    }

    private void onConnectSuccess() {
        this.setState(State.CONNECTED);
        this.fetchThreadState.setStateId(FetchThreadState.StateId.CONNECT_SUCCESS);
        this.fetchThread.enqueueMessage(this.fetchThreadState);
    }

    private void onConnectFailure(Throwable throwable) {
        log.warn(
            "Fetch thread connect reader server fail, thread will sleep " +
                NettyConstants.CONNECT_ERROR_SLEEP_INTERVAL +
                "ms before reconnect, ",
            throwable
        );

        try {
            Thread.sleep(NettyConstants.CONNECT_ERROR_SLEEP_INTERVAL);

            if (!this.getState()
                .equals(State.CLOSING) && !this.getState()
                .equals(State.CLOSED)) {
                this.fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
                this.fetchThread.enqueueMessage(this.fetchThreadState);
                this.setState(State.CLOSED);
            }

        } catch (InterruptedException ex) {
            log.error(
                "Connect thread sleep error.",
                ex
            );
        }
    }

    private void onSendRequestFailure(
        HttpRequest request,
        Throwable cause
    ) {
        log.error(
            "Fetch thread send request fail.",
            cause
        );
        this.fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
        this.fetchThread.enqueueMessage(this.fetchThreadState);
    }

    private void onChannelClose() {
        log.info("Fetch thread connection is closing.");
        this.setState(State.CLOSED);

        this.channel = null;
        this.fetchThread = null;
        this.fetchThreadState = null;
    }

    private void onChannelDisconnect() {
        log.warn("Fetch thread disconnected from remote server " + targetServer);

        if (!this.getState()
            .equals(State.CLOSING) && !this.getState()
            .equals(State.CLOSED)) {

            this.fetchThreadState.setStateId(FetchThreadState.StateId.PICK_SERVER);
            this.fetchThread.enqueueMessage(this.fetchThreadState);
            this.setState(State.CLOSED);
        } else {
            log.warn("Fetch thread connection is already closed.");
        }
    }

    private void processSubscribeResponse(FullHttpResponse httpResponse) {
        if (log.isDebugEnabled()) {
            log.debug("TaskName " + fetchThread.getTaskName() + ", SubscribeResponse Channel Id: " +
                          this.channel.id());
        }

        String result = httpResponse.content()
            .toString(CharsetUtil.UTF_8);
        if ("success".equals(result)) {
            this.fetchThreadState.setStateId(FetchThreadState.StateId.SUBSCRIBE_SUCCESS);
        } else {
            this.fetchThreadState.setStateId(FetchThreadState.StateId.SUBSCRIBE_FAILURE);
        }

        this.fetchThread.enqueueMessage(this.fetchThreadState);
    }

    private void processFetchMessagesResponse(FullHttpResponse httpResponse) {
        long timeCost = System.currentTimeMillis() - fetchThread.getLastFetchTime();
        if (log.isDebugEnabled()) {
            log.debug(
                "TaskName " + fetchThread.getTaskName() + ", FetchMessagesResponse Channel Id: " +
                    this.channel.id());
        }

        try {
            byte[] payloadBytes = new byte[httpResponse.content()
                .readableBytes()];
            httpResponse.content()
                .readBytes(payloadBytes);
            RdsEnvelope rdsEnvelope = RdsEnvelope.parseFrom(payloadBytes);
            if (rdsEnvelope.getErrorCode() != 0) {
                this.fetchThreadState.setStateId(FetchThreadState.StateId.FETCH_EVENTS_FAILURE);
                log.error("[fetch][error] Fetch message error: " + rdsEnvelope.getErrorMessage() + ";" + timeCost);
            } else {
                this.fetchThreadState.setStateId(FetchThreadState.StateId.FETCH_EVENTS_SUCCESS);
                this.fetchThreadState.setRdsEnvelope(rdsEnvelope);
            }
            this.inboundBytes += payloadBytes.length;
        } catch (InvalidProtocolBufferException e) {
            this.fetchThreadState.setStateId(FetchThreadState.StateId.FETCH_EVENTS_FAILURE);
            log.error(
                "Fetch thread parse reader server response fail.",
                e
            );
        }

        this.fetchThread.enqueueMessage(this.fetchThreadState);
    }


    public long getInboundBytes() {
        return inboundBytes;
    }

    public long getOutboundBytes() {
        return outboundBytes;
    }

    public ServerInfo getTargetServer() {
        return targetServer;
    }

    public void setTargetServer(ServerInfo targetServer) {
        this.targetServer = targetServer;
    }

    public static String getServerInfoURI() {
        return PROTOCOL_VERSION + SERVER_INFO_PATH;
    }

    public static String getSuscribeURI() {
        return PROTOCOL_VERSION + SUBSCRIBE_PATH;
    }

    public static String getFetchMessagesURI() {
        return PROTOCOL_VERSION + FETCH_MESSAGES_PATH;
    }

    public class RdsCdcChannelCloseListener implements ChannelCloseListener {

        @Override
        public void onChannelClose() {
            NettyHttpRdsCdcReaderConnection.this.onChannelClose();
        }
    }

    public class RdsCdcChannelDisconnectListener implements ChannelDisconnectListener {

        @Override
        public void onChannelDisconnect() {
            NettyHttpRdsCdcReaderConnection.this.onChannelDisconnect();
        }
    }

    public class RdsCdcSendRequestResultListener implements SendRequestResultListener {

        @Override
        public void onSendRequestSuccess(HttpRequest request) {

        }

        @Override
        public void onSendRequestFailure(
            HttpRequest request,
            Throwable cause
        ) {
            NettyHttpRdsCdcReaderConnection.this.onSendRequestFailure(
                request,
                cause
            );
        }
    }

    public class SubscribeResponseProcessor implements FullHttpResponseProcessor {

        @Override
        public void process(FullHttpResponse fullHttpResponse) throws Exception {
            NettyHttpRdsCdcReaderConnection.this.processSubscribeResponse(fullHttpResponse);
        }
    }

    public class FetchMessagesResponseProcessor implements FullHttpResponseProcessor {

        @Override
        public void process(FullHttpResponse fullHttpResponse) throws Exception {
            NettyHttpRdsCdcReaderConnection.this.processFetchMessagesResponse(fullHttpResponse);
        }
    }

    /**
     * Notifies about channel disconnect
     */
    public interface ChannelDisconnectListener {

        /**
         * Notifies about channel disconnect
         */
        void onChannelDisconnect();
    }

    /**
     * Notifies about channel close
     */
    public interface ChannelCloseListener {

        /**
         * Notifies about channel close
         */
        void onChannelClose();
    }

    /**
     * Notifies for the result of sending requests
     */
    public interface SendRequestResultListener {

        /**
         * Notifies about success of sending the specified request
         */
        void onSendRequestSuccess(HttpRequest req);

        /**
         * Notifies about failure of sending the specified request with the given cause
         */
        void onSendRequestFailure(
            HttpRequest req,
            Throwable cause
        );
    }

    public interface FullHttpResponseProcessor {

        void process(FullHttpResponse fullHttpResponse) throws Exception;
    }
}
