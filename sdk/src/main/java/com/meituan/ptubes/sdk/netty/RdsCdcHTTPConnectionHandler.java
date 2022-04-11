package com.meituan.ptubes.sdk.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCountUtil;



public class RdsCdcHTTPConnectionHandler extends ChannelDuplexHandler {

    private String taskName;
    private ChannelPromise promise;
    private ChannelHandlerContext ctx;

    private NettyHttpRdsCdcReaderConnection.SendRequestResultListener sendRequestResultListener;
    private NettyHttpRdsCdcReaderConnection.ChannelDisconnectListener channelDisconnectListener;
    private NettyHttpRdsCdcReaderConnection.FullHttpResponseProcessor fullHttpResponseProcessor;

    public RdsCdcHTTPConnectionHandler(String taskName) {
        this.taskName = taskName;
    }

    public void setSendRequestResultListener(NettyHttpRdsCdcReaderConnection.SendRequestResultListener sendRequestResultListener) {
        this.sendRequestResultListener = sendRequestResultListener;
    }

    public void setFullHttpResponseProcessor(NettyHttpRdsCdcReaderConnection.FullHttpResponseProcessor fullHttpResponseProcessor) {
        this.fullHttpResponseProcessor = fullHttpResponseProcessor;
    }

    public void setChannelDisconnectListener(NettyHttpRdsCdcReaderConnection.RdsCdcChannelDisconnectListener channelDisconnectListener) {
        this.channelDisconnectListener = channelDisconnectListener;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        channelDisconnectListener.onChannelDisconnect();
    }

    public ChannelPromise sendMessage(Object message, NettyHttpRdsCdcReaderConnection.FullHttpResponseProcessor fullHttpResponseProcessor) {
        if (ctx == null) {
            throw new IllegalStateException();
        }
        this.setFullHttpResponseProcessor(fullHttpResponseProcessor);
        promise = ctx.writeAndFlush(message).channel().newPromise();
        promise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {

                if (future.isSuccess()) {
                    sendRequestResultListener.onSendRequestSuccess((FullHttpRequest) message);
                } else {
                    sendRequestResultListener.onSendRequestFailure((FullHttpRequest) message, promise.cause());
                }
            }
        });

        return promise;
    }


    @Override
    public void channelRead(
        ChannelHandlerContext ctx,
        Object msg
    ) throws Exception {
        if (msg instanceof FullHttpResponse) {
            FullHttpResponse httpResponse = (FullHttpResponse) msg;

            this.fullHttpResponseProcessor.process(httpResponse);
            promise.trySuccess();
        }
        ReferenceCountUtil.release(msg);
    }

    public String getTaskName() {
        return taskName;
    }
}
