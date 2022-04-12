package com.meituan.ptubes.sdk.cluster.zk;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.checkpoint.ICheckpointPersistenceProvider;
import com.meituan.ptubes.sdk.checkpoint.PartitionStorage;
import org.I0Itec.zkclient.IZkDataListener;
import com.meituan.ptubes.sdk.consumer.FetchThread;


public class ZKRdsCdcClusterPartitionNumListener implements IZkDataListener {

    private final Logger log; 

    private String taskName;
    private FetchThread fetchThread;
    private ICheckpointPersistenceProvider checkpointPersistenceProvider;

    public ZKRdsCdcClusterPartitionNumListener(
        String taskName,
        FetchThread fetchThread,
        ICheckpointPersistenceProvider checkpointPersistenceProvider
    ) {
        this.taskName = taskName;
        this.fetchThread = fetchThread;
        this.checkpointPersistenceProvider = checkpointPersistenceProvider;
        log = LoggerFactory.getLoggerByTask(ZKRdsCdcClusterManager.class, this.taskName);
    }

    @Override
    public void handleDataChange(
        String dataPath,
        Object data
    ) throws Exception {
        log.info("Partition storage data changed, about to update checkpoint and partition number.");
        PartitionStorage partitionStorage = checkpointPersistenceProvider.loadPartitionStorage();

        if (partitionStorage == null) {
            return;
        }

        if (!fetchThread.needUpdatePartitionTotal(partitionStorage.getPartitionNum())) {
            String message = "Target partition number equal to current partition number. Ignore update partition number message.";
            log.warn(message);
            return;
        }

        fetchThread.updateCurrentCheckpoint(
            partitionStorage.getBuffaloCheckpoint(),
            false,
            false
        );
        fetchThread.updatePartitionTotal(partitionStorage.getPartitionNum());
        fetchThread.setNeedResubscribe(true);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {

    }
}
