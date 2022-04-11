package com.meituan.ptubes.sdk.cluster.zk;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterPartitionNotifier;
import org.apache.helix.participant.statemachine.StateModelFactory;



public class ZKRdsCdcClusterNotifierFactory extends
    StateModelFactory<ZKRdsCdcClusterNotifierStateModel> {

    private String taskName;
    private IRdsCdcClusterPartitionNotifier notifier;
    private final Logger log;

    public ZKRdsCdcClusterNotifierFactory(IRdsCdcClusterPartitionNotifier notifier, String taskName) {
        this.taskName = taskName;
        this.notifier = notifier;
        log = LoggerFactory.getLoggerByTask(ZKRdsCdcClusterNotifierFactory.class, taskName);
    }

    @Override
    public synchronized ZKRdsCdcClusterNotifierStateModel createNewStateModel(
        String resourceName,
        String partitionKey
    ) {
        log.info("Creating a new callback object for partition=" + partitionKey);
        return new ZKRdsCdcClusterNotifierStateModel(
                taskName,
            partitionKey,
            notifier
        );
    }
}
