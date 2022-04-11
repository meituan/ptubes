package com.meituan.ptubes.reader.container.network.connections;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.network.cache.BinaryEvent;
import com.meituan.ptubes.reader.container.network.cache.EventCache;
import com.meituan.ptubes.reader.container.network.request.GetRequest;
import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.manager.SessionManager;
import com.meituan.ptubes.reader.container.network.request.sub.SubRequest;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;

public class Session extends AbstractClientSession {
    private static final Logger LOG = LoggerFactory.getLogger(Session.class);

    private final SessionManager parent;
    private final Channel channel;
    private final String hostName;
    private final ReaderTaskStatMetricsCollector readerTaskCollector;
    private final SubRequest subRequest;
    private final StorageConfig storageConfig;

    private AtomicBoolean resetRequest = new AtomicBoolean(false);
    private AtomicBoolean shutdownRequest = new AtomicBoolean(false);
    private volatile AsyncOpResListener reSubListener;
    private SessionExtractor sessionExtractor;
    private SessionTransmitter sessionTransmitter;
    private EventCache<BinaryEvent> eventCache;
    private Phaser resetPhaser;
    private Phaser shutdownPhaser;

    public Session(
        SessionManager sessionManager,
        String readerTaskName,
        SourceType sourceType,
        String clientId,
        Channel channel,
        SubRequest subRequest,
        StorageConfig storageConfig,
        ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector
    ) {
        super(clientId, readerTaskName, sourceType);
        this.parent = sessionManager;
        this.channel = channel;
        this.hostName = ((InetSocketAddress)channel.remoteAddress()).getHostName();
        this.subRequest = subRequest;
        this.storageConfig = storageConfig;
        this.readerTaskCollector = readerTaskStatMetricsCollector;
    }

    public String getHostName() {
        return hostName;
    }

    public boolean hasShutdownRequest() {
        return shutdownRequest.get();
    }

    public boolean hasResetRequest() {
        return resetRequest.get();
    }

    private boolean  sessionFinalize() {
        boolean res = true;
        try {
            eventCache.clear();
            channel.close();
            LOG.info("session {} resources have been released", this.clientId);
        } catch (Throwable te) {
            LOG.error("shutdown session {} error", clientId, te);
            res = false;
        }
        return res;
    }

    /**
     * Finally executed by ExtractorThread or TransmitterThread
     * @return
     */
    private boolean reset() {
        assert hasResetRequest() == true;

        boolean res = true;
        try {
            // Only public variables are released here, private variables are released within each thread
            this.eventCache.clear();
            LOG.info("reset session {} sucessfully", clientId);
        } catch (Throwable te) {
            LOG.error("reset session {} error", clientId, te);
            this.closeAsync();
            res = false;
        } finally {
            try {
                resetRequest.set(false);
                if (reSubListener != null) {
                    // reset result callback client
                    this.reSubListener.operationComplete(res);
                    this.reSubListener = null;
                }
            } catch (Throwable te) {
                LOG.error("carry out reset finally block error", te);
            }
        }
        return res;
    }

    @Override
    public void startup() {
        if (isStart() == false) {
            try {
                this.resetPhaser = new Phaser(2) {
                    
                    @Override
                    public boolean onAdvance(int phase, int registeredParties) {
                        boolean resetRes = false;
                        try {
                            LOG.info("reset phase={} registeredParties = {}", phase, registeredParties);
                            if (registeredParties == 2) {
                                resetRes = Session.this.reset();
                            }
                        } catch (Throwable te) {
                            LOG.error("reset session {} in reset barrier error", clientId,  te);
                            Session.this.closeAsync();
                            resetRes = false;
                        }
                        return !resetRes || super.onAdvance(phase, registeredParties);
                    }
                };
                this.shutdownPhaser = new Phaser(2) {
                    @Override
                    public boolean onAdvance(int phase, int registeredParties) {
                        try {
                            LOG.info("shutdown phase={} registeredParties = {}", phase, registeredParties);
                            Session.this.sessionFinalize();
                        } catch (Throwable te) {
                            LOG.error("shutdown session {} in shutdown barrier error", clientId, te);
                        }
                        return super.onAdvance(phase, registeredParties);
                    }
                };
                eventCache = new EventCache<>(
                    ContainerConstants.MAX_EVENT_CACHE_NUM,
                    ContainerConstants.MAX_EVENT_CACHE_SIZE
                );

                this.sessionExtractor = new SessionExtractor(
                    resetPhaser,
                    shutdownPhaser,
                    this,
                    subRequest,
                    storageConfig,
                    eventCache,
                    readerTaskCollector
                );
                this.sessionTransmitter = new SessionTransmitter(
                    resetPhaser,
                    shutdownPhaser,
                    this,
                    channel,
                    eventCache,
                    subRequest
                );
                this.sessionExtractor.init();
                this.sessionTransmitter.init();
                this.sessionExtractor.start();
                this.sessionTransmitter.start();

                super.start();

                LOG.info("session {} start!", clientId);
            } catch (Throwable te) {
                LOG.error("session {} startup error", clientId, te);
                throw new PtubesRunTimeException("session start error", te);
            }
        }
    }

    private void shutdownAsynchronously() {
        if (shutdownRequest.compareAndSet(false, true)) {
            LOG.info("send shutdown request to session {}", clientId);
        } else {
            LOG.warn("session {} shutdown more than once", clientId);
        }
    }
    private void shutdownInSync() throws InterruptedException {
        shutdownAsynchronously();
        sessionExtractor.waitForShutdownFinish();
        sessionTransmitter.waitForShutdownFinish();
    }
    private boolean shutdownInSyncWithTimeout(long timeout, TimeUnit timeUnit) throws InterruptedException {
        shutdownAsynchronously();
        return sessionExtractor.waitForShutdownFinish(timeout, timeUnit) && sessionTransmitter.waitForShutdownFinish(timeout, timeUnit);
    }

    @Override
    public boolean offerGetRequest(GetRequest getRequest) {
        return sessionTransmitter.offerGetRequest(getRequest);
    }

    @Override
    public synchronized void resetAsynchronously(SubRequest newSubRequest, AsyncOpResListener reSubListener) {
        // [issue-1.2.0] resetRequest reset flag must trigger the switch after configuration update, otherwise there will be configuration conflicts
        if (shutdownRequest.get() == false && resetRequest.get() == false) {
            this.reSubListener = reSubListener;
            sessionExtractor.resetSettings(newSubRequest);
            sessionTransmitter.resetSettings(newSubRequest);
            resetRequest.set(true);
        } else {
            reSubListener.operationComplete(false);
        }
    }

    @Override
    public synchronized Session closeAsync() {
        if (super.stop() == true) {
            this.shutdownAsynchronously();
            LOG.info("session {} stop in async successfully", clientId);
        } else {
            LOG.error("session {} has gone", clientId);
        }
        return this;
    }
    @Override
    public void waitForClose() throws InterruptedException {
        sessionExtractor.waitForShutdownFinish();
        sessionTransmitter.waitForShutdownFinish();
    }
    @Override
    public boolean waitForCloseWithTimeout(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return sessionExtractor.waitForShutdownFinish(timeout, timeUnit) && sessionTransmitter.waitForShutdownFinish(timeout, timeUnit);
    }

    @Override
    public synchronized void closeInSync() throws InterruptedException {
        if (super.stop() == true) {
            this.shutdownInSync();
            LOG.info("session {} stop in sync successfully", clientId);
        } else {
            LOG.error("session {} has not setup yet", clientId);
        }
    }
    @Override
    public synchronized boolean closeInSyncWithTimeout(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (super.stop() == true) {
            boolean res = this.shutdownInSyncWithTimeout(timeout, timeUnit);
            LOG.info("session {} stop in sync {}", clientId, res ? "successfully" : "unsuccessfully");
            return res;
        } else {
            return true;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("[ clientId=").append(clientId)
            .append(", channel=").append(channel.id().asShortText())
            .append(", subRequest=").append(subRequest.toString()).append(" ]");
        return sb.toString();
    }
}
