package com.meituan.ptubes.sdk.consumer;

import com.google.protobuf.ByteString;
import com.meituan.ptubes.common.exception.RdsCdcRuntimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.IRdsCdcEventListener;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.CheckpointPersistenceProviderFactory;
import com.meituan.ptubes.sdk.checkpoint.ICheckpointPersistenceProvider;
import com.meituan.ptubes.sdk.model.PartitionClusterInfo;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.meituan.ptubes.sdk.config.WorkThreadConfig;
import com.meituan.ptubes.sdk.protocol.RdsPacket;

public class WorkThreadCluster implements IControllableActor {

    // private static final String WORK_CLUSTER_PREFIX = "ptubes-worker-";
    private static final String WORK_CLUSTER_PREFIX = "rds-cdc-worker-";

    private String taskName;
    private String clusterName;
    private PartitionClusterInfo readOnlyPartitionClusterInfo; // Used to share shard information in fetchThread

    private final Logger log;

    private WorkThreadConfig workThreadConfig;
    private IRdsCdcEventListener rdsCdcEventListener;
    private ICheckpointPersistenceProvider checkpointPersistenceProvider;

    private Map<Integer, WorkThread> workThreadMap = new ConcurrentHashMap<>();
    private Map<Integer, UncaughtExceptionTrackingThread> workWrappedThreadMap = new ConcurrentHashMap<>();

    public WorkThreadCluster(
        String taskName,
        WorkThreadConfig workThreadConfig,
        IRdsCdcEventListener rdsCdcEventListener
    ) {
        this.log = LoggerFactory.getLoggerByTask(
            WorkThreadCluster.class,
            taskName
        );

        this.taskName = taskName;
        this.clusterName = WORK_CLUSTER_PREFIX + taskName;

        this.workThreadConfig = workThreadConfig;
        this.rdsCdcEventListener = rdsCdcEventListener;
    }

    public synchronized void addWorkThread(int partitionId) {
        if (workThreadMap.containsKey(partitionId) && workWrappedThreadMap.containsKey(partitionId)) {
            log.warn(String.format(
                "Partition %s already existed, the add thread operation will skip.",
                partitionId
            ));
            return;
        }

        if (workThreadMap.containsKey(partitionId) || workWrappedThreadMap.containsKey(partitionId)) {
            String errorMessage = String.format(
                "Partition %s is in illegal state, workThreadMap and workWrappedThreadMap should have the same partition key",
                partitionId
            );
            RdsCdcRuntimeException ex = new RdsCdcRuntimeException(errorMessage);

            throw ex;
        }

        WorkThread workThread = new WorkThread(
                partitionId,
                clusterName + "-" + partitionId,
                workThreadConfig,
                rdsCdcEventListener,
                this.checkpointPersistenceProvider,
                this.readOnlyPartitionClusterInfo
        );
        workThread.enqueueMessage(LifecycleMessage.createStartMessage());

        workThreadMap.put(
            partitionId,
            workThread
        );

        workWrappedThreadMap.put(
            partitionId,
            new UncaughtExceptionTrackingThread(
                workThread,
                workThread.getName(),
                taskName
            )
        );
    }

    public synchronized void dropWorkThread(int partitionId) {
        if (!workThreadMap.containsKey(partitionId) && !workWrappedThreadMap.containsKey(partitionId)) {
            log.warn(String.format(
                "Partition %s already deleted, the drop thread operation will skip.",
                partitionId
            ));
            return;
        }

        if (!workThreadMap.containsKey(partitionId) || !workWrappedThreadMap.containsKey(partitionId)) {
            String errorMessage = String.format(
                "Partition %s is in illegal state, workThreadMap and workWrappedThreadMap should have the same partition key",
                partitionId
            );
            RdsCdcRuntimeException ex = new RdsCdcRuntimeException(errorMessage);

            throw ex;
        }

        workThreadMap.remove(partitionId);
        workWrappedThreadMap.remove(partitionId);
    }

    public void dispatchMessage(
        int partitionId,
        RdsPacket.RdsMessage rdsMessage
    ) {
        WorkThread workThread = workThreadMap.get(partitionId);

        // If helix shrinks the shards, it is possible to get null here, call back the storage point and the new number of shards through the helix callback, and backtrack the data to ensure that the data is not lost
        if (workThread != null) {
            for (ByteString eventString : rdsMessage.getMessagesList()) {
                workThread.addEventString(eventString);
            }
        }
    }

    public synchronized void start() {
        try {
            this.checkpointPersistenceProvider = CheckpointPersistenceProviderFactory.getInstance(workThreadConfig.getTaskName());
        } catch (Exception e) {
            throw new RdsCdcRuntimeException(e);
        }

        // In theory, the threads in the workThreadCluster should be empty when initialized, all shards should be added by helix at runtime, and this logic should be reserved for future expansion
        for (UncaughtExceptionTrackingThread workWrappedThread : workWrappedThreadMap.values()) {
            workWrappedThread.setDaemon(true);
            workWrappedThread.start();
        }
    }

    public synchronized void startPartition(int partitionId) {
        UncaughtExceptionTrackingThread workWrappedThread = workWrappedThreadMap.get(partitionId);

        if (null != workWrappedThread && !workWrappedThread.isAlive()) {
            workWrappedThread.setDaemon(true);
            workWrappedThread.start();
        }
    }

    @Override
    public synchronized void pause() {
        for (WorkThread workThread : workThreadMap.values()) {
            workThread.pause();
        }
    }

    @Override
    public synchronized void resume() {
        for (WorkThread workThread : workThreadMap.values()) {
            workThread.resume();
        }
    }

    @Override
    public synchronized void shutdown() {
        for (WorkThread workThread : workThreadMap.values()) {
            workThread.shutdown();
        }

        if (this.checkpointPersistenceProvider != null) {
            CheckpointPersistenceProviderFactory.shutdown(taskName);
        }
    }

    public void awaitShutdown() {
        for (WorkThread workThread : workThreadMap.values()) {
            UncaughtExceptionTrackingThread workWrappedThread = workWrappedThreadMap.get(workThread.getPartitionId());

            if (workWrappedThread.isAlive()) {
                workThread.awaitShutdown();
            }
        }
    }

    public synchronized void shutdown(int partitionId) {
        WorkThread workThread = workThreadMap.get(partitionId);

        if (null == workThread) {
            return;
        }

        workThread.shutdown();
    }

    public void awaitShutdown(int partitionId) {
        WorkThread workThread = workThreadMap.get(partitionId);
        UncaughtExceptionTrackingThread workWrappedThread = workWrappedThreadMap.get(partitionId);

        if (null == workThread) {
            return;
        }

        if (workWrappedThread != null && workWrappedThread.isAlive()) {
            workThread.awaitShutdown();
        }
    }

    public synchronized BuffaloCheckpoint getEarliestCheckpoint() {
        BuffaloCheckpoint earliestCheckpoint = null;

        for (WorkThread workThread : workThreadMap.values()) {
            BuffaloCheckpoint currentCheckpoint = workThread.getCopiedCurrentCheckpoint();

            if (currentCheckpoint.compareTo(earliestCheckpoint) < 0) {
                earliestCheckpoint = currentCheckpoint;
            }
        }

        return earliestCheckpoint;
    }

    public synchronized void updateWorkThreadConfig(WorkThreadConfig workThreadConfig) {
        this.workThreadConfig = workThreadConfig;
        for (WorkThread workThread : this.workThreadMap.values()) {
            workThread.updateWorkThreadConfig(workThreadConfig);
        }
    }

    public WorkThreadConfig getWorkThreadConfig() {
        return workThreadConfig;
    }

    public Map<Integer, WorkThread> getWorkThreadMap() {
        return workThreadMap;
    }

    public String getTaskName() {
        return this.taskName;
    }

    public String getClusterName() {
        return this.clusterName;
    }

    public PartitionClusterInfo getReadOnlyPartitionClusterInfo() {
        return readOnlyPartitionClusterInfo;
    }

    public void setReadOnlyPartitionClusterInfo(PartitionClusterInfo readOnlyPartitionClusterInfo) {
        this.readOnlyPartitionClusterInfo = readOnlyPartitionClusterInfo;
    }
}
