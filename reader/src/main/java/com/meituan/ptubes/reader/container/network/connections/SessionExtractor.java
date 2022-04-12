package com.meituan.ptubes.reader.container.network.connections;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.reader.container.network.cache.BinaryEvent;
import com.meituan.ptubes.reader.container.network.cache.EventCache;
import com.meituan.ptubes.reader.container.network.encoder.EncoderType;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.ErrorEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.network.request.sub.SubRequest;
import com.meituan.ptubes.reader.container.utils.TypeCastUtil;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.channel.DefaultReadChannel;
import com.meituan.ptubes.reader.storage.channel.ReadChannel;
import com.meituan.ptubes.reader.storage.filter.PartitionEventFilter;
import com.meituan.ptubes.reader.storage.filter.SourceAndPartitionEventFilter;
import com.meituan.ptubes.sdk.model.DatabaseInfo;
import com.meituan.ptubes.sdk.model.PartitionClusterInfo;

public class SessionExtractor extends SessionBaseThread {

    private static final Logger LOG = LoggerFactory.getLogger(SessionExtractor.class);

    private final Session session;
    private volatile SubRequest currentSubConfig;
    private volatile SubRequest nextSubConfig;
    private final StorageConfig storageConfig;
    private final ReaderTaskStatMetricsCollector readerTaskCollector;
    private final EventCache<BinaryEvent> eventCache;

    // runtime
    private ReadChannel readChannel;
    private boolean isPreviousCommit = true;
    private boolean isHeartBeatCommit = false;
    private BroadcastCache broadcastCache = null;
    private Function<BinaryEvent, Boolean> broadcastFunc;

    private static boolean breakLoop(Session session) {
        if (Thread.currentThread().isInterrupted()) {
            return true;
        }
        if (session.hasShutdownRequest() || session.hasResetRequest()) {
            return true;
        }
        return false;
    }

    public SessionExtractor(
        Phaser resetPhaser,
        Phaser shutdownPhaser,
        Session session,
        SubRequest initSubConfig,
        StorageConfig storageConfig,
        EventCache<BinaryEvent> eventCache,
        ReaderTaskStatMetricsCollector readerTaskCollector
    ) {
        super(
            session,
            session.getClientId() + "_extractor",
            resetPhaser,
            shutdownPhaser);
        this.session = session;
        this.currentSubConfig = initSubConfig;
        this.storageConfig = storageConfig;
        this.eventCache = eventCache;
        this.readerTaskCollector = readerTaskCollector;
    }

    private ReadChannel openReadChannel(BuffaloCheckpoint checkpoint, SourceAndPartitionEventFilter eventFilter) throws Exception {
        ReadChannel readChannel = new DefaultReadChannel(storageConfig, eventFilter, readerTaskCollector, session.sourceType);
        readChannel.start();
        try {
            switch (checkpoint.getCheckpointMode()) {
                case NORMAL:
                    BinlogInfo binlogInfo = TypeCastUtil.checkpointToBinlogInfo(checkpoint);
                    readChannel.open(binlogInfo);
                    break;
                case EARLIEST:
                    readChannel.openOldest();
                    break;
                case LATEST:
                default:
                    readChannel.openLatest();
                    break;
            }
            return readChannel;
        } catch (Exception rcOpenException) {
            readChannel.stop();
            throw rcOpenException;
        }
    }

    private SourceAndPartitionEventFilter genEventFilter(Set<DatabaseInfo> databaseInfoSet, PartitionClusterInfo partitionClusterInfo) {
        Map<String, PartitionEventFilter> sourceFilters = new HashMap<>((int)(databaseInfoSet.size() * 1.34));
        PartitionEventFilter partitionEventFilter = new PartitionEventFilter(false,
            new HashSet<>(partitionClusterInfo.getPartitionSet()), partitionClusterInfo.getPartitionTotal());
        for (DatabaseInfo databaseInfo : databaseInfoSet) {
            String dbName = databaseInfo.getDatabaseName();
            for (String tableName : databaseInfo.getTableNames()) {
                sourceFilters.put(dbName + "." + tableName, partitionEventFilter);
            }
        }
        SourceAndPartitionEventFilter eventFilter = new SourceAndPartitionEventFilter(sourceFilters);
        return eventFilter;
    }

    @Override
    protected void init() throws Exception {
        String clientId = session.getClientId();
        try {
            this.broadcastFunc = (binaryEvent) -> {
                try {
                    boolean applyRes = eventCache.put(
                        binaryEvent,
                        200,
                        TimeUnit.MILLISECONDS
                    );

                    return applyRes;
                } catch (InterruptedException ie) {
                    Thread.interrupted(); // keep interruptted flag
                    return false;
                }

            };
            Set<DatabaseInfo> databaseInfoSet = currentSubConfig.getServiceGroupInfo().getDatabaseInfoSet();
            PartitionClusterInfo partitionClusterInfo = currentSubConfig.getPartitionClusterInfo();
            BuffaloCheckpoint checkpoint = currentSubConfig.getBuffaloCheckpoint();

            SourceAndPartitionEventFilter eventFilter = genEventFilter(databaseInfoSet, partitionClusterInfo);
            this.readChannel = openReadChannel(checkpoint, eventFilter);
        } catch (IOException ioe) {
            LOG.error("session {} reopen read channel error", clientId, ioe);
            throw ioe;
        } catch (LessThanStorageRangeException ltsre) {
            LOG.error("session {} resub ckpt less than buffer range", clientId, ltsre);
            throw ltsre;
        } catch (GreaterThanStorageRangeException gtsre) {
            LOG.error("session {} resub ckpt greater than buffer range", clientId, gtsre);
            throw gtsre;
        } catch (PtubesException be) {
            LOG.error("session {} reopen read channel error", clientId, be);
            throw be;
        } catch (Throwable te) {
            LOG.error("session {} reset error, close session", clientId, te);
            throw te;
        }
    }

    public void resetSettings(SubRequest newSubConfig) {
        this.nextSubConfig = newSubConfig;
    }
    @Override
    protected boolean resetRun() throws Exception {
        boolean res = false;

        if (nextSubConfig == null) {
            LOG.error("{} has no configs for resub", session.getClientId());
            throw new PtubesException("resub request is invalid");
        }

        String clientId = session.getClientId();
        SubRequest targetSubConfig = nextSubConfig;
        this.currentSubConfig = targetSubConfig;
        this.nextSubConfig = null;

        LOG.info("session {} extractor start to reset", clientId);

        try {
            readChannel.stop();
            broadcastCache = null;

            Set<DatabaseInfo> databaseInfoSet = targetSubConfig.getServiceGroupInfo().getDatabaseInfoSet();
            PartitionClusterInfo partitionClusterInfo = targetSubConfig.getPartitionClusterInfo();
            BuffaloCheckpoint checkpoint = targetSubConfig.getBuffaloCheckpoint();

            SourceAndPartitionEventFilter eventFilter = genEventFilter(databaseInfoSet, partitionClusterInfo);
            this.readChannel = openReadChannel(checkpoint, eventFilter);

            LOG.info("session {} extractor reset sucessfully!", clientId);
            res = true;
        } catch (IOException e) {
            LOG.error("session {} reopen read channel error", clientId, e);
            res = false;
        } catch (LessThanStorageRangeException e) {
            LOG.error("session {} resub ckpt less than buffer range", clientId, e);
            res = false;
        } catch (GreaterThanStorageRangeException e) {
            LOG.error("session {} resub ckpt greater than buffer range", clientId, e);
            res = false;
        } catch (PtubesException e) {
            LOG.error("session {} reopen read channel error", clientId, e);
            res = false;
        } catch (Throwable te) {
            LOG.error("session {} reset error, close session", clientId, te);
            res = false;
        }

        return res;
    }

    // Change here to be careful to filter out some events
    private boolean filter(PtubesEvent storageEvent) throws PtubesException, InterruptedException {
        if (EventType.isErrorEvent(storageEvent.getEventType())) {
            if (storageEvent == ErrorEvent.NO_MORE_EVENT) {
                Thread.sleep(20);  // Reduce the call frequency of readChannel.next
                return true;
            } else {
                
                LOG.error("session {} receive error event: {}", session.getClientId(), storageEvent.getEventType().name());
                throw new PtubesException("session receive error event from storage");
            }
        }

        // 2019.9.4 requires support for filtering ddl/commit events
        if (currentSubConfig.isNeedDDL() == false) {
            if (storageEvent.getEventType().equals(EventType.DDL)) {
                LOG.debug("skipping ddl event");
                return true;
            }
        }
        if (currentSubConfig.isNeedEndTransaction() == false) {
            if (storageEvent.getEventType().equals(EventType.COMMIT)) {
                return true;
            }
        }

        // Continuous commit time is not issued
        boolean isHeartBeatEvent = EventType.HEARTBEAT.equals(storageEvent.getEventType());
        switch (storageEvent.getEventType()) {
            case COMMIT:
                // 2020.3.17 requires the removal of the heartbeat table commit, the heartbeat table and the business table cannot be in one transaction
                if (isHeartBeatCommit) {
                    // Heartbeat commit and other transaction commits do not interfere with each other
                    isHeartBeatCommit = false;
                    return true;
                }

                if (isPreviousCommit) {
                    return true;
                } else {
                    isPreviousCommit = true;
                }
                break;
            default:
                // 2020.3.17 Prevent heartbeat event from restoring isPreviousCommit state
                if (isHeartBeatEvent == false) {
                    isPreviousCommit = false;
                }
                break;
        }
        isHeartBeatCommit = isHeartBeatEvent;
        return false;
    }
    private BroadcastCache encodeAndCacheEvent(final PtubesEvent storageEvent) throws UnsupportedEncodingException {
        BroadcastCache broadcastCache = null;

        BinaryEvent binaryEventTemplate;
        byte[] pbEncodedData = storageEvent.getPayload();
        switch (storageEvent.getEventType()) {
            case HEARTBEAT:
            case DDL:
            case COMMIT:
                // Broadcast ddl, commit, heartbeat events with little memory loss
                isHeartBeatCommit = EventType.HEARTBEAT.equals(storageEvent.getEventType());
                binaryEventTemplate = new BinaryEvent(-1, (short)-1, storageEvent.getBinlogInfo(), EncoderType.PROTOCOL_BUFFER, pbEncodedData, storageEvent);
                broadcastCache = new BroadcastCache(session, binaryEventTemplate, currentSubConfig.getPartitionClusterInfo().getPartitionSet(), false);
                break;
            default:
                // dml
                int partitionId = storageEvent.getPartitionId() % currentSubConfig.getPartitionClusterInfo().getPartitionTotal();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} {} extract event, type={}, table={}, partKey={}, hash={}, ts={}",
                        session.getHostName(), session.getClientId(),
                        storageEvent.getEventType().name(), storageEvent.getTableName(),
                        storageEvent.isKeyNumber() ? storageEvent.getKey() : new String(storageEvent.getKeyBytes()),
                        storageEvent.getPartitionId(),
                        storageEvent.getTimestampInNS());
                }
                binaryEventTemplate = new BinaryEvent(partitionId, storageEvent.getPartitionId(), storageEvent.getBinlogInfo(), EncoderType.PROTOCOL_BUFFER, pbEncodedData, storageEvent);
                broadcastCache = new BroadcastCache(session, binaryEventTemplate, new HashSet<>(Arrays.asList(partitionId)), true);
                break;
        }

        return broadcastCache;
    }
    @Override
    protected boolean runOnce() throws Exception {
        boolean res;
        try {
            // No new events will be read until the last event broadcast is complete
            if (broadcastCache != null) {
                boolean broadcastRes = broadcastCache.broadcast(broadcastFunc);
                if (broadcastRes) {
                    broadcastCache = null;
                }
            } else {
                PtubesEvent storageEvent = readChannel.next();

                if (filter(storageEvent)) {
                    // skip or not, maybe throw exception, if skip, do nothing
                } else {
                    assert broadcastCache == null;
                    broadcastCache = encodeAndCacheEvent(storageEvent);
                }
            }
            res = true;
        } catch (InterruptedException ie) {
            LOG.error("session {} extractor is interrupted", session.getClientId(), ie);
            Thread.currentThread().interrupt();
            res = false;
        } catch (IOException ioe) {
            
            LOG.error("session {} extractor occur io exception, session close", session.getClientId(), ioe);
            session.closeAsync();
            res = false;
        } catch (Throwable te) {
            LOG.error("session {} caught unknown exception, session close", session.getClientId(), te);
            session.closeAsync();
            res = false;
        }
        return res;
    }

    @Override
    protected boolean breakLoopRun() {
        return true;
    }

    @Override
    protected boolean shutdownRun() {
        String clientId = session.getClientId();
        LOG.info("session {} extractor start to shutdown...", clientId);
        readChannel.stop();
        broadcastCache = null; // unlink gc-root
        LOG.info("session {} extractor is already terminated!", clientId);
        return true;
    }

    @Override
    protected void finalRun() {
        // do nothing
    }

    // thread-unsafe
    private final static class BroadcastCache {

        private final Session parent;
        private final BinaryEvent broadcastEvent;
        private final List<Integer> partitionIds;
        private final ListIterator<Integer> iterator;
        private final boolean passThrough;

        public BroadcastCache(
            Session parent,
            BinaryEvent broadcastEvent,
            Set<Integer> partitionIds,
            boolean passThrough
        ) {
            this.parent = parent;
            this.broadcastEvent = broadcastEvent;
            this.partitionIds = new ArrayList<>(partitionIds.size());
            this.partitionIds.addAll(partitionIds);
            this.iterator = this.partitionIds.listIterator();
            this.passThrough = passThrough;
        }

        private boolean needFastLoop() {
            return breakLoop(parent);
        }

        /**
         * // isShutdown can only be from false to true and can be synchronized externally
         */
        public boolean broadcast(Function<BinaryEvent, Boolean> function) throws InterruptedException {
            while (needFastLoop() == false && iterator.hasNext()) {
                Integer partitionId = iterator.next();
                BinaryEvent binaryEvent;
                // Here is a linkage. If iterator.size()>1, then passThrough is false, so the binaryEvent will be copied and put into the cache. Otherwise, it is directly put into the original object (because only one copy is needed)
                if (passThrough) {
                    binaryEvent = this.broadcastEvent;
                } else {
                    binaryEvent = this.broadcastEvent.shallowCopiedBinaryEvent(partitionId);
                }
                boolean functionRes = function.apply(binaryEvent);
                if (functionRes == false) {
                    iterator.previous(); // write failed, backtrack forward
                }
            }
            return iterator.hasNext() == false;
        }
    }

}
