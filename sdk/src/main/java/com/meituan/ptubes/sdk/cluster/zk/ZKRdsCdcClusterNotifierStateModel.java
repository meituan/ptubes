package com.meituan.ptubes.sdk.cluster.zk;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterPartitionNotifier;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;



@StateModelInfo(initialState = "OFFLINE",
    states = {
        "ONLINE",
        "ERROR",
        "DROPPED"
    })
public class ZKRdsCdcClusterNotifierStateModel extends StateModel {

    private final Logger log;
    private final String taskName;
    private final IRdsCdcClusterPartitionNotifier notifier;
    private final String partition;

    public ZKRdsCdcClusterNotifierStateModel(
        String taskName,
        String partition,
        IRdsCdcClusterPartitionNotifier notifier
    ) {
        this.taskName = taskName;
        this.notifier = notifier;
        String[] ps = partition.split("_");
        if (ps.length >= 2) {
            this.partition = ps[ps.length - 1];
        } else {
            this.partition = "-1";
        }
        log = LoggerFactory.getLoggerByTask(
            ZKRdsCdcClusterNotifierStateModel.class,
            taskName
        );
    }

    @Transition(to = "ONLINE",
        from = "OFFLINE")
    public synchronized void onBecomeOnlineFromOffline(
        Message message,
        NotificationContext context
    ) {
        log.info(taskName + " partition (" + partition + ") become online from offline !!");
        if (notifier != null) {
            notifier.onGainedPartitionOwnership(Integer.parseInt(partition));
        }
        log.info("state model transition finished !!");
    }

    @Transition(to = "OFFLINE",
        from = "ONLINE")
    public synchronized void onBecomeOfflineFromOnline(
        Message message,
        NotificationContext context
    ) {
        log.info(taskName + " partition (" + partition + ") become offline from online !!");
        if (notifier != null) {
            notifier.onLostPartitionOwnership(Integer.parseInt(partition));
        }
        log.info("state model transition finished !!");
    }

    @Transition(to = "DROPPED",
        from = "OFFLINE")
    public synchronized void onBecomeDroppedFromOffline(
        Message message,
        NotificationContext context
    ) {
        log.info(taskName + " partition (" + partition + ") become dropped from offline !!");
        if (notifier != null) {
            notifier.onLostPartitionOwnership(Integer.parseInt(partition));
        }
        log.info("state model transition finished !!");
    }

    @Transition(to = "OFFLINE",
        from = "ERROR")
    public synchronized void onBecomeOfflineFromError(
        Message message,
        NotificationContext context
    ) {
        log.info(taskName + " partition (" + partition + ") become offline from error !!");
        if (notifier != null) {
            notifier.onError(Integer.parseInt(partition));
        }
        log.info("state model transition finished !!");
    }

    @Override
    public void reset() {
        if (notifier != null) {
            notifier.onReset(Integer.parseInt(partition));
        }
    }

}
