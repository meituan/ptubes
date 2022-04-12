package com.meituan.ptubes.sdk.consumer;

import com.google.protobuf.ByteString;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.AckStatus;
import com.meituan.ptubes.sdk.AckType;
import com.meituan.ptubes.sdk.IRdsCdcEventListener;
import com.meituan.ptubes.sdk.RdsCdcEventStatus;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.ICheckpointPersistenceProvider;
import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.RdsCdcCheckpointFactory;
import com.meituan.ptubes.sdk.constants.ConsumeConstants;
import com.meituan.ptubes.sdk.constants.MonitorConstants;
import com.meituan.ptubes.sdk.model.PartitionClusterInfo;
import com.meituan.ptubes.sdk.model.wrapper.AckEventWrapper;
import com.meituan.ptubes.sdk.monitor.WorkMonitorInfo;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.meituan.ptubes.sdk.config.PtubesSdkConsumerConfig;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import com.meituan.ptubes.sdk.config.WorkThreadConfig;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import com.meituan.ptubes.sdk.utils.CommonUtil;

public class WorkThread extends AbstractActor {

    private String taskName;
    private final RdsCdcSourceType sourceType;
    private final AckType ackType;
    private final PartitionClusterInfo readOnlyPartitionClusterInfo; // reference to fetchThread.readerConnectionConfig.partitionClusterInfo
    private final long waitStageThreshold; // Waiting for the threshold of the ack stage, the timeout will be retransmitted
    private int partitionId;
    private long workEventCount;
    private int retryConfig;
    private int retryCount;
    private volatile int batchSize;
    private volatile int batchTimeoutMs;
    private volatile int checkpointSyncIntervalMs;

    private BlockingQueue<ByteString> eventStringQueue;
    private IRdsCdcEventListener rdsCdcEventListener;
    private WorkThreadState workThreadState;
    private WorkThreadConfig workThreadConfig;
    private ICheckpointPersistenceProvider checkpointPersistenceProvider;
    private DelayStrategy delayStrategy;
    private DelayStrategy ackDelayStrategy;
    private volatile String lastSendUuid; // The uuid of the last call service waiting for ack eg. 0017-1628836211000-1 Fragment number - fragment start time - single increment id
    private volatile String lastAckUuid; // The uuid of the last business callback ack eg. 0017-1628836211000-1 Shard number-shard start time-single increment id // Not used for now, exact match
    private volatile long lastSendTimestamp; // Need ack scenario, time-consuming waiting for business ack (inaccurate, will be reset when retransmitted overtime)
    private final String indexPrefix; // eg. 0017-1628836211000-
    private long indexForThread = 0L; // Increment the batch id within the same worker, single-threaded write without locking
    // private volatile long indexForBatch = 0L; // Number of retransmissions of the same batch (timeout ACK) within the same worker Single-threaded write without locking
    private long lastBatchTimestamp;

    private AtomicReference<BuffaloCheckpoint> currentCheckpoint = new AtomicReference<>();

    public WorkThread(
        int partitionId,
        String name,
        WorkThreadConfig workThreadConfig,
        IRdsCdcEventListener rdsCdcEventListener,
        ICheckpointPersistenceProvider checkpointPersistenceProvider,
        PartitionClusterInfo readOnlyPartitionClusterInfo
    ) {
        super(
            name,
            LoggerFactory.getLoggerByTask(
                name,
                workThreadConfig.getTaskName()
            )
        );

        updateWorkThreadConfig(workThreadConfig);

        this.sourceType = workThreadConfig.getSourceType(); // Dynamic modification is not supported, put it outside updateWorkThreadConfig
        this.ackType = workThreadConfig.getAckType(); // Dynamic modification is not supported, put it outside updateWorkThreadConfig
        this.waitStageThreshold = 10 * 1000; // 10 s
        this.readOnlyPartitionClusterInfo = readOnlyPartitionClusterInfo;
        this.partitionId = partitionId;
        this.rdsCdcEventListener = rdsCdcEventListener;
        this.eventStringQueue = new LinkedBlockingQueue<>(ConsumeConstants.WORKTHREAD_BLOCKING_QUEUE_SIZE);
        this.workThreadState = new WorkThreadState(this.taskName);
        this.checkpointPersistenceProvider = checkpointPersistenceProvider;
        this.currentCheckpoint.set(this.checkpointPersistenceProvider.loadCheckpoint(partitionId));
        this.indexPrefix = String.format("%04d", this.partitionId) + "-" + System.currentTimeMillis() + "-"; // Fragment field fixed length 4 characters

        this.retryCount = 0;
        this.lastBatchTimestamp = System.currentTimeMillis();
        this.delayStrategy = new LinearDelayStrategy(
            0,
            2,
            1,
            1000
        );
        if (AckType.ACK == ackType) {
            ackDelayStrategy = new LinearDelayStrategy(
                    0,
                    2,
                    1,
                    500
            );
        }
    }

    public synchronized void addEventString(ByteString rdsEventString) {

        boolean isPut = false;

        while (!isPut && !isShutdownRequested()) {
            try {
                int workerQueueSize = eventStringQueue.size();
                if (log.isDebugEnabled()) {
                    log.debug("[worker][" + this.partitionId + "][queue][size]" + workerQueueSize);
                }
                isPut = this.eventStringQueue.offer(
                    rdsEventString,
                    ConsumeConstants.WORKTHREAD_OFFER_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS
                );
            } catch (Throwable ex) {
                log.error("Failed to put message into dispatch thread queue, ex: " + ex);
            }
        }
    }

    @Override
    protected boolean executeAndChangeState(Object message) {
        boolean success = true;

        if (message instanceof WorkThreadState) {
            if (this.status != Status.RUNNING) {
                log.warn("Actor not running: " + message.toString());
            } else {
                WorkThreadState workThreadState = (WorkThreadState) message;

                switch (workThreadState.getStateId()) {
                    case START:
                        break;
                    case POLL_EVENTS:
                        doPollEvents(workThreadState);
                        break;
                    case START_CONSUME:
                        doStartConsume(workThreadState);
                        break;
                    case WAIT_FOR_ACK:
                        doWaitForAck(workThreadState);
                        break;
                    case CONSUME_SUCCESS:
                        doConsumeSuccess(workThreadState);
                        break;
                    case STORE_CHECKPOINT:
                        doStoreCheckpoint(workThreadState);
                        break;
                    case STORE_CHECKPOINT_SUCCESS:
                        doStoreCheckpointSuccess(workThreadState);
                        break;
                    default: {
                        log.error("Unknown state in work thread: " + workThreadState.getStateId());
                        success = false;
                        break;
                    }
                }
            }
        } else {
            success = super.executeAndChangeState(message);
        }

        return success;
    }

    private void handleRdsCdcHeartbeat(RdsPacket.RdsEvent rdsEvent) {
        try {
            RdsPacket.Column utime = rdsEvent.getRowData()
                .getAfterColumnsMap()
                .get(MonitorConstants.HEARTBEAT_COLUMN_NAME);

            long heartbeatTime = CommonUtil.transformToPhysicalTime(Long.parseLong(utime.getValue()),sourceType);
            this.workThreadState.setHeartbeatTimestamp(heartbeatTime);
            if (null == this.sourceType || RdsCdcSourceType.MYSQL == this.sourceType) {
                this.workThreadState.setHeartbeatCheckpoint(RdsCdcCheckpointFactory.buildFromPBCheckpoint(
                        this.taskName, rdsEvent.getHeader().getCheckpoint()));
                this.workThreadState.setCheckpoint(rdsEvent.getHeader()
                        .getCheckpoint());
            } else {
                throw new RuntimeException("unknown sourceType handleRdsCdcHeartbeat:" + this.sourceType);
            }
        } catch (Exception e) {
            log.error(
                "update work thread state heartbeat info failed.",
                e
            );
        }
    }

    private void doPollEvents(WorkThreadState workThreadState) {
        try {
            ByteString byteString = eventStringQueue.poll(
                ConsumeConstants.WORKTHREAD_POLL_TIMEOUT_MS,
                TimeUnit.MILLISECONDS
            );
            if (byteString != null) {
                RdsPacket.RdsEvent rdsEvent = RdsPacket.RdsEvent.parseFrom(byteString);
                long executeTs = CommonUtil.transformToPhysicalTime(rdsEvent.getHeader().getExecuteTime(), sourceType);
                long duration = System.currentTimeMillis() - executeTs;
                if (rdsEvent.getEventType()
                    .equals(RdsPacket.EventType.HEARTBEAT)) {

                    handleRdsCdcHeartbeat(rdsEvent);

                    /**
                     * In the case of batch sending, if there is data in workThreadState, the heartbeat storage point cannot be stored.
                     * Prevent the cached data from being unconsumed during downtime, and re-subscribe and consume from the heartbeat storage point after shard migration
                     */
                    if (workThreadState.getRdsEvents()
                        .isEmpty()) {
                        workThreadState.setStateId(WorkThreadState.StateId.STORE_CHECKPOINT);
                    } else {
                        workThreadState.setStateId(WorkThreadState.StateId.POLL_EVENTS);
                    }
                    enqueueMessage(workThreadState);
                    return;
                }

                workThreadState.addRdsEvent(rdsEvent);
            }

            if (workThreadState.getRdsEvents()
                .isEmpty()) {
                workThreadState.setStateId(WorkThreadState.StateId.POLL_EVENTS);
            } else if (workThreadState.getRdsEvents()
                .size() < this.batchSize &&
                System.currentTimeMillis() < (this.lastBatchTimestamp + this.batchTimeoutMs)) {
                workThreadState.setStateId(WorkThreadState.StateId.POLL_EVENTS);
            } else {
                workThreadState.setStateId(WorkThreadState.StateId.START_CONSUME);
            }
            enqueueMessage(workThreadState);
        } catch (Throwable e) {
            log.error(
                "WorkThread " + getName() + " consume event with exception: ",
                e
            );
        }
    }

    private void doStartConsume(WorkThreadState workThreadState) {
        retryCount++;
        String idForAck = null;
        LinkedList<RdsPacket.RdsEvent> rdsEvents = workThreadState.getRdsEvents();
        RdsCdcEventStatus rdsCdcEventStatus = RdsCdcEventStatus.FAILURE;

        long startTime = System.currentTimeMillis();
        // 1, According to the processing type, start executing the downstream consumption logic
        try {
            if (null == ackType || AckType.WITHOUT_ACK == ackType) {
                rdsCdcEventStatus = doNoneAckInvoke(rdsEvents);
            } else {
                idForAck = genBatchUuid();
                rdsCdcEventStatus =  doAckInvoke(rdsEvents, idForAck);
            }
        } catch (Throwable e) {
            String errorMessage = "Writer process event fail.";

            log.error(
                errorMessage,
                e
            );
            rdsCdcEventStatus = RdsCdcEventStatus.FAILURE;
        } finally {
            long timeCount = System.currentTimeMillis() - startTime;
            int failCount = RdsCdcEventStatus.FAILURE.equals(rdsCdcEventStatus) ? rdsEvents.size() : 0;
            if (ackType == AckType.ACK) {
                log.info("[ivk] " + idForAck + ";" + rdsCdcEventStatus + ";" + rdsEvents.size() + ";" + failCount + ";" + timeCount);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format(
                "Event execute result: %s, %s time to consume, left times %s.",
                rdsCdcEventStatus.getName(),
                retryCount,
                retryConfig - retryCount
            ));
        }

        // 2, If downstream consumption fails, retry processing with state machine, retryCount == retryConfig
        if (rdsCdcEventStatus == RdsCdcEventStatus.FAILURE) {

            if (retryCount < retryConfig || retryConfig == -1) {
                delayStrategy.sleep();

                workThreadState.setStateId(WorkThreadState.StateId.START_CONSUME);
                enqueueMessage(workThreadState);
                return; // failed direct retry
            }
        }

        retryCount = 0;
        delayStrategy.reset();
        lastBatchTimestamp = System.currentTimeMillis();

        // 3, if the downstream consumption is successful, enter the next state machine
        if (rdsCdcEventStatus == RdsCdcEventStatus.SUCCESS) {
            if (null == ackType || AckType.WITHOUT_ACK == ackType) {
                // // No need for ack 1. Directly clear the queue for this consumption 2. Save the storage point to the state machine
                finishingUpForDoStartConsumeSuccessOrAckSuccess(rdsEvents, workThreadState);
            } else {
                // Need ack 1. Keep the data in rdsEvents, 2. Clear the data after ack + keep the storage point
                lastSendUuid = idForAck; // There is only one write
                lastSendTimestamp = System.currentTimeMillis();
                workThreadState.setStateId(WorkThreadState.StateId.WAIT_FOR_ACK);
            }

        } else {
            // 4, (not SUCCESS or FAILURE), do not update the storage point, continue to pull data
            // SKIP scene directly clears the queue for this consumption
            workEventCount += rdsEvents.size();
            rdsEvents.clear();
            workThreadState.setStateId(WorkThreadState.StateId.POLL_EVENTS);
        }

        enqueueMessage(workThreadState);

    }


    /**
     * 1. Clear the current queue to be consumed
     * 2. Advance the storage point forward
     * 3. Advance memory technology count (for log)
     * @param workThreadState
     */
    private void doWaitForAck(WorkThreadState workThreadState) {
        boolean ackSuccess = false;
        long startTs = System.currentTimeMillis();
        while (true) {
            if (this.lastSendUuid.equals(lastAckUuid)) {
                ackSuccess = true;
                break;
            } else {
                if (System.currentTimeMillis() - startTs > waitStageThreshold) {
                    break;
                } else {
                    ackDelayStrategy.sleep();
                }
            }
        }
        ackDelayStrategy.reset();
        if (ackSuccess) {
            // acked
            finishingUpForDoStartConsumeSuccessOrAckSuccess(workThreadState.getRdsEvents(), workThreadState);
            indexForThread++; // only one write, do not lock first
        } else {
            // timed out, re-push
            workThreadState.setStateId(WorkThreadState.StateId.START_CONSUME);
        }
        enqueueMessage(workThreadState);

    }

    private void doConsumeSuccess(WorkThreadState workThreadState) {
        workThreadState.setStateId(WorkThreadState.StateId.STORE_CHECKPOINT);
        enqueueMessage(workThreadState);
    }

    private void doStoreCheckpoint(WorkThreadState workThreadState) {
        BuffaloCheckpoint checkpoint = workThreadState.getCheckpoint();

        long currentTimeMillis = System.currentTimeMillis();
        if (checkpoint != null && currentTimeMillis - workThreadState.getLastZKStoreTime() >=
            this.checkpointSyncIntervalMs) {
            checkpointPersistenceProvider.storeCheckpointIfExists(
                getPartitionId(),
                checkpoint
            );
            log.info("[ckp][store]" + checkpoint);
            workThreadState.setLastZKStoreTime(currentTimeMillis);
        }

        workThreadState.setStateId(WorkThreadState.StateId.STORE_CHECKPOINT_SUCCESS);
        enqueueMessage(workThreadState);
    }

    private void doStoreCheckpointSuccess(WorkThreadState workThreadState) {
        if (log.isDebugEnabled()) {
            log.debug("Work thread " + partitionId + " event total count: " + workEventCount);
        }

        this.currentCheckpoint.set(workThreadState.getCheckpoint());
        workThreadState.setStateId(WorkThreadState.StateId.POLL_EVENTS);
        enqueueMessage(workThreadState);
    }

    /**
     * Trigger timing:
     * 1. After the synchronization task calls the user successfully
     * 2. After asynchronous task ack
     * content:
     * 1. Advance storage point
     * 2. Clear the queue to be consumed
     * 3. Update memory count
     */
    private void finishingUpForDoStartConsumeSuccessOrAckSuccess(LinkedList<RdsPacket.RdsEvent> rdsEvents, WorkThreadState workThreadState) {
        if (null == workThreadConfig.getSourceType() || RdsCdcSourceType.MYSQL == workThreadConfig.getSourceType()) {
            RdsPacket.Checkpoint lastCheckpoint = rdsEvents.getLast().getHeader().getCheckpoint();
            workEventCount += rdsEvents.size();
            rdsEvents.clear();
            workThreadState.setCheckpoint(lastCheckpoint);
        }else {
            throw new RuntimeException("unknown sourceType in finishingUpForDoStartConsumeSuccessOrAckSuccess:" + workThreadConfig.getSourceType());
        }
        workThreadState.setStateId(WorkThreadState.StateId.CONSUME_SUCCESS);
    }

    private RdsCdcEventStatus doAckInvoke(LinkedList<RdsPacket.RdsEvent> rdsEvents, String idForAck) {
        AckEventWrapper ackEventWrapper = new AckEventWrapper();
        ackEventWrapper.setEvents(rdsEvents);
        ackEventWrapper.setPartitionTotal(readOnlyPartitionClusterInfo.getPartitionTotal());
        ackEventWrapper.setCurrentPartition(partitionId);
        ackEventWrapper.setBatchUuid(idForAck);
        RdsCdcEventStatus status = rdsCdcEventListener.onEventsWithAck(ackEventWrapper);
        return status;
    }

    private RdsCdcEventStatus doNoneAckInvoke(LinkedList<RdsPacket.RdsEvent> rdsEvents) {
        return rdsCdcEventListener.onEvents(rdsEvents);
    }

    private String genBatchUuid() {
        return indexPrefix + indexForThread ;
    }

    public AckStatus ack(String idForAck) {
        long ackTimeCost = System.currentTimeMillis() - lastSendTimestamp; // No lock processing, imprecise, used for statistical investigation, the actual value is greater than the recorded value
        ackTimeCost = ackTimeCost < 0 ? 0L : ackTimeCost;
        if (this.status != Status.RUNNING) {
            log.error("[ack] partition not exist3 idForAck:" + idForAck + "; partition:" + partitionId);
            return AckStatus.FAIL_NOT_EXISTENT_PARTITION;
        }
        if (this.ackType != AckType.ACK) {
            log.error("[ack] no need for ack:" + idForAck);
            return AckStatus.FAIL_NO_NEED_FOR_ACK;
        }

        if (!idForAck.equals(this.lastSendUuid)) { // No synchronization, no problem for now
            String lastSendPrefix = this.lastSendUuid.substring(0, 19);
            if (idForAck.substring(0, 19).equals(lastSendPrefix)) {
                // 1. Not equal, and the startup timestamp is consistent
                try {
                    String lastSendIndex = this.lastSendUuid.substring(19);
                    if (Integer.parseInt(idForAck.substring(19)) <= Integer.parseInt(lastSendIndex)) {
                        // 1.a idForAck.id <= lastSendPrefix.index proves that the ack has passed this time. Pass directly
                        log.info("[ack] ok1a:" + idForAck + "" + lastAckUuid);
                        this.lastAckUuid = lastSendUuid;
                        return AckStatus.OK;
                    } else {
                        // 1.b idForAck.id > lastSendPrefix.index proves that this data has not been sent yet (theoretically cannot appear)
                        log.error("[ack] ack failure1b:" + idForAck + ":" + this.lastSendUuid);
                        return AckStatus.FAIL;
                    }
                } catch (NumberFormatException e) {
                    log.error("[ack] ack failure1e1:" + idForAck + ":" + this.lastSendUuid);
                    return AckStatus.FAIL_WRONG_ACK_ID;
                } catch (Exception e) {
                    log.error("[ack] ack failure1e2:" + idForAck + ":" + this.lastSendUuid);
                    return AckStatus.FAIL;
                }

            } else {
                // 2. Not equal but inconsistent startup timestamps (maybe shard migration, etc.)
                log.info("[ack] ok2:" + idForAck + "" + lastAckUuid);
                this.lastAckUuid = lastSendUuid;
                return AckStatus.OK;
            }

        } else {
            this.lastAckUuid = idForAck;
            log.info("[ack] ok:" + idForAck);
            return AckStatus.OK;
        }
    }

    @Override
    public void doStart(LifecycleMessage lifecycleMessage) {
        log.info("work thread start, name: " + getName());
        if (workThreadState.getStateId() != WorkThreadState.StateId.START) {
            return;
        }

        super.doStart(lifecycleMessage);
        workThreadState.setStateId(WorkThreadState.StateId.POLL_EVENTS);
        enqueueMessage(workThreadState);
    }

    @Override
    protected void onShutdown() {
        this.checkpointPersistenceProvider = null;
    }

    public synchronized void updateWorkThreadConfig(WorkThreadConfig workThreadConfig) {
        this.workThreadConfig = workThreadConfig;

        this.taskName = workThreadConfig.getTaskName();

        // FailureMode = skip means that after one execution, regardless of success or failure, the data will be discarded and continue to consume
        // FailureMode = retry, retryTimes > 1, which means that if the execution of retryTimes fails, the data will be discarded and continue to be consumed
        // FailureMode = retry, retryTimes = -1, which means that if the execution fails, it will always retry
        this.retryConfig = workThreadConfig.getFailureMode() == PtubesSdkConsumerConfig.FailureMode.RETRY
            ? workThreadConfig.getRetryTimes() : 1;
        this.batchSize = PtubesSdkConsumerConfig.ConsumptionMode.BATCH.equals(workThreadConfig.getConsumptionMode())
            ? workThreadConfig.getBatchSize() : 1;
        this.batchTimeoutMs = workThreadConfig.getBatchTimeoutMs();
        this.checkpointSyncIntervalMs = workThreadConfig.getCheckpointSyncIntervalMs();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public WorkThreadConfig getWorkThreadConfig() {
        return workThreadConfig;
    }

    public BuffaloCheckpoint getCopiedCurrentCheckpoint() {
        BuffaloCheckpoint checkpoint = currentCheckpoint.get();
        if (null == workThreadConfig.getSourceType() || RdsCdcSourceType.MYSQL == workThreadConfig.getSourceType()) {
            return getMysqlCopiedCurrentCheckpoint((MysqlCheckpoint) checkpoint);
        }else {
            throw new RuntimeException("unknown sourceType in getCopiedCurrentCheckpoint:" + workThreadConfig.getSourceType());
        }
    }

    public MysqlCheckpoint getMysqlCopiedCurrentCheckpoint(MysqlCheckpoint checkpoint) {
        MysqlCheckpoint copiedCheckpoint = new MysqlCheckpoint();

        if (checkpoint == null) {
            copiedCheckpoint.setVersionTs(-1);
            return copiedCheckpoint;
        }

        copiedCheckpoint.setCheckpointMode(checkpoint.getCheckpointMode());
        copiedCheckpoint.setEventIndex(checkpoint.getEventIndex());
        copiedCheckpoint.setBinlogOffset(checkpoint.getBinlogOffset());
        copiedCheckpoint.setBinlogFile(checkpoint.getBinlogFile());
        copiedCheckpoint.setServerId(checkpoint.getServerId());
        copiedCheckpoint.setUuid(checkpoint.getUuid());
        copiedCheckpoint.setTimestamp(checkpoint.getTimestamp());
        copiedCheckpoint.setTransactionId(checkpoint.getTransactionId());
        copiedCheckpoint.setVersionTs(checkpoint.getVersionTs());

        return copiedCheckpoint;
    }

    public WorkMonitorInfo<? extends BuffaloCheckpoint> getWorkMonitorInfo() {
        if (null == sourceType || sourceType == RdsCdcSourceType.MYSQL) {
            return getMysqlWorkMonitorInfo();
        }else {
            throw new RuntimeException("unknown sourceType in getWorkMonitorInfo:" + sourceType);
        }
    }

    public WorkMonitorInfo<MysqlCheckpoint> getMysqlWorkMonitorInfo() {
        long heartbeatTimestamp;
        MysqlCheckpoint heartbeatCheckpoint;
        MysqlCheckpoint latestCheckpoint;
        if (this.workThreadState.getCheckpoint() != null) {
            heartbeatTimestamp = this.workThreadState.getHeartbeatTimestamp();
            heartbeatCheckpoint = (MysqlCheckpoint)this.workThreadState.getHeartbeatCheckpoint();
            latestCheckpoint = (MysqlCheckpoint)this.workThreadState.getCheckpoint();
        } else {
            MysqlCheckpoint rdsCdcCheckpoint = (MysqlCheckpoint)this.currentCheckpoint.get();
            heartbeatTimestamp = rdsCdcCheckpoint.getTimestamp();
            heartbeatCheckpoint = rdsCdcCheckpoint;
            latestCheckpoint = rdsCdcCheckpoint;
        }
        WorkMonitorInfo<MysqlCheckpoint> workMonitorInfo = new WorkMonitorInfo<>();
        workMonitorInfo.setHeartbeatTimestamp(heartbeatTimestamp);
        workMonitorInfo.setHeartbeatCheckpoint(heartbeatCheckpoint);
        workMonitorInfo.setLatestCheckpoint(latestCheckpoint);
        return workMonitorInfo;
    }

}
