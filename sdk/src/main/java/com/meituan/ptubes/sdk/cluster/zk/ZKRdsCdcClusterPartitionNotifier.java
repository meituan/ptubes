package com.meituan.ptubes.sdk.cluster.zk;

import com.meituan.ptubes.common.exception.RdsCdcRuntimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.CheckpointPersistenceProviderFactory;
import com.meituan.ptubes.sdk.checkpoint.ICheckpointPersistenceProvider;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterPartitionNotifier;
import com.meituan.ptubes.sdk.consumer.FetchThread;
import com.meituan.ptubes.sdk.consumer.WorkThreadCluster;

public class ZKRdsCdcClusterPartitionNotifier implements IRdsCdcClusterPartitionNotifier {

    private final Logger log;

    private String taskName;
    private FetchThread fetchThread;
    private WorkThreadCluster workThreadCluster;
    private ICheckpointPersistenceProvider checkpointPersistenceProvider;

    public ZKRdsCdcClusterPartitionNotifier(
        FetchThread fetchThread,
        WorkThreadCluster workThreadCluster
    ) {
        this.taskName = fetchThread.getTaskName();
        this.log = LoggerFactory.getLoggerByTask(
            ZKRdsCdcClusterPartitionNotifier.class,
            taskName
        );

        this.fetchThread = fetchThread;
        this.workThreadCluster = workThreadCluster;

        try {
            this.checkpointPersistenceProvider = CheckpointPersistenceProviderFactory
                .getInstance(fetchThread.getTaskName());
        } catch (Exception e) {
            log.error(
                "CheckpointPersistenceProvider init error.",
                e
            );
            throw new RdsCdcRuntimeException(e);
        }
    }

    @Override
    public synchronized void onGainedPartitionOwnership(int partitionId) {
        log.info(taskName + " partition (" + partitionId + ") getting added !!");

        if (workThreadCluster.getWorkThreadMap()
            .containsKey(partitionId)) {
            log.info(taskName + " partition (" + partitionId + ") existed !!");
            return;
        }

        // Get the storage point of the shard on ZK
        BuffaloCheckpoint partitionCheckpoint = checkpointPersistenceProvider.loadCheckpoint(partitionId);
        log.info("Partition checkpoint info: " + partitionCheckpoint);

        // Get the earliest storage point in the work data stream
        BuffaloCheckpoint earliestCheckpoint = workThreadCluster.getEarliestCheckpoint();
        log.info("Earliest checkpoint info: " + earliestCheckpoint);

        // If the storage point of the shard is smaller than the earliest storage point of the data stream, update the subscription storage point of the fetch thread
        if (earliestCheckpoint == null || (partitionCheckpoint != null &&
            partitionCheckpoint.compareTo(earliestCheckpoint) < 0)) {
            earliestCheckpoint = partitionCheckpoint;
        }

        log.info("Update fetch checkpoint to " + earliestCheckpoint);
        fetchThread.updateCurrentCheckpoint(
            earliestCheckpoint,
            false,
            workThreadCluster.getWorkThreadMap()
                .isEmpty()
        );

        workThreadCluster.addWorkThread(partitionId);
        workThreadCluster.startPartition(partitionId);

        fetchThread.addPartition(partitionId);
        fetchThread.setNeedResubscribe(true);

        log.info(taskName + " partition (" + partitionId + ") getting added finished !!");
    }

    @Override
    public synchronized void onLostPartitionOwnership(int partitionId) {
        log.info(taskName + " partition (" + partitionId + ") getting removed !!");

        fetchThread.dropPartition(partitionId);
        fetchThread.setNeedResubscribe(true);

        workThreadCluster.shutdown(partitionId);
        workThreadCluster.awaitShutdown(partitionId);
        workThreadCluster.dropWorkThread(partitionId);

        log.info(taskName + " partition (" + partitionId + ") getting removed finished !!");
    }

    @Override
    public void onError(int partitionId) {
        log.error("Error notification received for partition " + partitionId);
        onLostPartitionOwnership(partitionId);
    }

    @Override
    public void onReset(int partitionId) {
        log.error("Reset notification received for partition " + partitionId);
        onLostPartitionOwnership(partitionId);
    }
}
