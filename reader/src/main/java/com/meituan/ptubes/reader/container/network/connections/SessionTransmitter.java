package com.meituan.ptubes.reader.container.network.connections;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.network.cache.BinaryEvent;
import com.meituan.ptubes.reader.container.network.cache.EventCache;
import com.meituan.ptubes.reader.container.network.encoder.EncoderType;
import com.meituan.ptubes.reader.container.network.encoder.ProtocalBufferEncoder;
import com.meituan.ptubes.reader.container.network.request.GetRequest;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.utils.NettyUtil;
import com.meituan.ptubes.reader.container.network.request.sub.SubRequest;
import com.meituan.ptubes.reader.monitor.collector.ClientSessionStatMetricsCollector;

public class SessionTransmitter extends SessionBaseThread {

    private static final Logger LOG = LoggerFactory.getLogger(SessionTransmitter.class);

    private final Session session;
    private final Channel channel;
    private final EventCache<BinaryEvent> eventCache;
    private final BlockingQueue<GetRequest> getRequests;
    private final List<BinaryEvent> outBuffer;
    private volatile SubRequest currentSubConfig;
    private volatile SubRequest nextSubConfig;

    public SessionTransmitter(
        Phaser resetPhaser,
        Phaser shutdownPhaser,
        Session session,
        Channel channel,
        EventCache<BinaryEvent> eventCache,
        SubRequest initSubConfig
    ) {
        super(
            session,
            session.getClientId() + "_transmitter",
            resetPhaser,
            shutdownPhaser);
        this.session = session;
        this.channel = channel;
        this.eventCache = eventCache;
        this.currentSubConfig = initSubConfig;
        this.getRequests = new LinkedBlockingQueue<>(1);
        this.outBuffer = new ArrayList<>();
    }

    public boolean offerGetRequest(GetRequest getRequest) {
        return this.getRequests.offer(getRequest);
    }

    @Override
    protected void init() throws Exception {
        // do nothing
    }

    public void resetSettings(SubRequest newSubConfig) {
        this.nextSubConfig = newSubConfig;
//        super.resetReq();
    }

    @Override
    protected boolean resetRun() throws Exception {
        LOG.info("session {} transmitter start to reset...", session.getClientId());
        this.currentSubConfig = nextSubConfig;
        this.nextSubConfig = null;
        getRequests.clear();
        outBuffer.clear();
        LOG.info("session {} transmitter reset sucessfully!", session.getClientId());
        return true;
    }

    @Override
    protected boolean runOnce() throws Exception {
        try {
            GetRequest req = getRequests.poll(200, TimeUnit.MILLISECONDS);

            int dataSize = 0;
            boolean continueOuterLoop = false;
            if (req != null) {
                long deadline = System.currentTimeMillis() + req.getTimeout();

                // Solution:
                // 1. Cut timeLeft into a fine-grained time slice, which has nothing to do with the client's timeout time
                long timeLeft = req.getTimeout();
                long waitTime = (timeLeft > 100 ? 100 : timeLeft);
                do {
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    if (session.hasResetRequest() || session.hasShutdownRequest()) {
                        continueOuterLoop = true; // Equivalent to break + label
                        break;
                    }

                    
                    BinaryEvent event = eventCache.get(waitTime, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        
                        if (EncoderType.PROTOCOL_BUFFER == currentSubConfig.getCodec()) {
                            PtubesEvent ptubesEvent = event.getOriData();
                            if (EventType.INSERT.equals(ptubesEvent.getEventType()) || EventType.UPDATE.equals(ptubesEvent.getEventType()) || EventType.DELETE.equals(ptubesEvent.getEventType())) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("{} {} transmit event, type={}, table={}, partKey={}, hash={}, ts={}",
                                        session.getHostName(), session.getClientId(),
                                        ptubesEvent.getEventType().name(),
                                        ptubesEvent.getTableName(),
                                        ptubesEvent.isKeyNumber() ? ptubesEvent.getKey() : new String(ptubesEvent.getKeyBytes()),
                                        ptubesEvent.getPartitionId(),
                                        ptubesEvent.getTimestampInNS());
                                }
                            }
                        } else {
                            throw new UnsupportedEncodingException("unsupported json encoding anymore");
                        }
                        outBuffer.add(event);
                        dataSize += event.getSize();

                        if (outBuffer.size() >= req.getEventNum() || dataSize >= req.getEventSize()) {
                            break;
                        }
                    }
                    timeLeft = deadline - System.currentTimeMillis(); // There is a performance penalty for concurrent calls to currentTimeMillis
                } while(timeLeft > 10);
                if (continueOuterLoop) {
                    return true;
                }

                byte[] packageRawData;
                switch (currentSubConfig.getCodec()) {
                    case PROTOCOL_BUFFER:
                        packageRawData = ProtocalBufferEncoder.INSTANCE.encodePackage(session.sourceType, outBuffer);
                        break;
                    case JSON:
                    case RAW:
                    default:
                        throw new UnsupportedEncodingException("unsupported encoding type " + currentSubConfig.getCodec().name());
                }

                DefaultFullHttpResponse response = NettyUtil.wrappedFullResponse(packageRawData);
                channel.writeAndFlush(response);

                ClientSessionStatMetricsCollector collector = NettyUtil.getChannelAttribute(
                    channel,
                    ContainerConstants.ATTR_CLIENTSESSION_COLLECTOR
                );
                if (collector != null) {
                    if (outBuffer.size() > 0) {
                        collector.setLastGetBinlogInfo(outBuffer.get(outBuffer.size() - 1).getBinlogInfo());
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.error("clientsession collector of channel {} is not exist", channel.id().asShortText());
                    }
                }
                outBuffer.clear();
            } else {
                // do nothing
                LOG.debug("no get data request");
            }
        } catch (InterruptedException ie) {
            LOG.error("session {} transmitter is interrupted", session.getClientId(), ie);
            Thread.currentThread().interrupt();
        } catch (Throwable te) {
            LOG.error("session {} caught exception", session.getClientId(), te);
            session.closeAsync();
        }

        return true;
    }

    @Override
    protected boolean breakLoopRun() {
        return true;
    }

    @Override
    protected boolean shutdownRun() {
        String clientId = session.getClientId();
        LOG.info("session {} transmitter start to shutdown...", clientId);
        getRequests.clear();
        outBuffer.clear();
        LOG.info("session {} transmitter is already terminated!", clientId);
        return true;
    }

    @Override
    protected void finalRun() {
        // do nothing
    }
}
