package com.meituan.ptubes.sdk.cluster.zk;

import com.meituan.ptubes.common.exception.RdsCdcRuntimeException;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.CheckpointPersistenceProviderFactory;
import com.meituan.ptubes.sdk.checkpoint.ICheckpointPersistenceProvider;
import com.meituan.ptubes.sdk.checkpoint.PartitionStorage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterController;
import com.meituan.ptubes.sdk.config.PtubesSdkSubscriptionConfig;
import com.meituan.ptubes.sdk.config.RdsCdcClusterConfig;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import com.meituan.ptubes.sdk.config.notification.IConfigChangeNotifier;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;


public class ZKRdsCdcClusterController
    implements IRdsCdcClusterController, ExternalViewChangeListener {

    private final Logger log;

    private String taskName;
    private String zkAddr;
    private RdsCdcSourceType rdsCdcSourceType;

    private HelixAdmin admin;
    private HelixManager helixManager;
    private ICheckpointPersistenceProvider checkpointPersistenceProvider;
    private IConfigChangeNotifier configChangeNotifier;

    public ZKRdsCdcClusterController(RdsCdcClusterConfig config) {
        log = LoggerFactory.getLoggerByTask(
            ZKRdsCdcClusterController.class,
            config.getTaskName()
        );

        taskName = config.getTaskName();
        zkAddr = config.getZkAddr();
        rdsCdcSourceType = config.getRdsCdcSourceType();
        this.configChangeNotifier = config.getConfigChangeNotifier();

        try {
            this.checkpointPersistenceProvider = CheckpointPersistenceProviderFactory.getInstance(taskName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void startController() {
        if (helixManager == null) {
            String controllerId = "controller_" + new Random(System.currentTimeMillis()).nextInt();
            helixManager = HelixControllerMain.startHelixController(
                zkAddr,
                taskName,
                controllerId,
                HelixControllerMain.STANDALONE
            );
            admin = helixManager.getClusterManagmentTool();

            try {
                helixManager.addControllerListener(this);
                helixManager.addExternalViewChangeListener(this);
            } catch (Exception e) {
                log.error(
                    "Cluster start controller fail: ",
                    e
                );
                throw new RdsCdcRuntimeException(e);
            }
        }
        if (admin != null) {
            admin.enableCluster(
                helixManager.getClusterName(),
                true
            );
        }
    }

    @Override
    public void stopController() {
        if (helixManager != null) {
            helixManager.disconnect();
        }

        if (admin != null) {
            admin.close();
        }
    }

    @Override
    public void pauseController() {
        if (admin != null) {
            admin.enableCluster(
                taskName,
                false
            );
        }
    }

    @Override
    public void resumeController() {
        if (admin != null) {
            admin.enableCluster(
                taskName,
                true
            );
        }
    }


    @Override
    public void onControllerChange(NotificationContext changeContext) {
        if (changeContext.getType() == Type.FINALIZE) {
            log.info("Helix controller has changed, but change context type is " + Type.FINALIZE +
                         ", do nothing, task name: " + taskName);
            return;
        }

        if (!isLeader()) {
            log.info(
                "Helix controller has changed, but current member is not leader, do nothing, task name: " + taskName);
            return;
        }

        PartitionStorage partitionStorage = checkpointPersistenceProvider.loadPartitionStorage();
        if (partitionStorage == null) {
            log.info(
                "Helix controller has changed, but partition storage is null, will not update partition number, task name: " +
                    taskName);
            return;
        }

        int latestPartitionNum = configChangeNotifier.getConfig(null, PtubesSdkSubscriptionConfig.class).getPartitionNum();
        IdealState idealState = admin.getResourceIdealState(
            taskName,
            ZKRdsCdcClusterManager.DEFAULT_RESOURCE_NAME
        );

        if (latestPartitionNum != partitionStorage.getPartitionNum()) {
            updatePartitionNum(latestPartitionNum);
        } else if (idealState.getNumPartitions() != partitionStorage.getPartitionNum()) {
            updatePartitionNum(partitionStorage.getPartitionNum());
        }
    }

    @Override
    public void onExternalViewChange(
        List<ExternalView> externalViewList,
        NotificationContext changeContext
    ) {
        try {
            if (externalViewList.size() != 1) {
                return;
            }
            log.info(taskName + " partition change.");
            ExternalView externalView = externalViewList.get(0);
            for (String partitionName : externalView.getPartitionSet()) {
                Map<String, String> stateMap = externalView.getStateMap(partitionName);
                for (Entry<String, String> stateMapEntry : stateMap.entrySet()) {
                    String[] ps = partitionName.split("_");
                    String partition;
                    if (ps.length >= 2) {
                        partition = ps[ps.length - 1];
                    } else {
                        partition = "-1";
                    }

                    String instanceName = stateMapEntry.getKey();
                    String status = stateMapEntry.getValue();
                    log.info(String.format(
                        "partition %s on instance: %s , status: %s",
                        partition,
                        instanceName,
                        status
                    ));
                }
            }
        } catch (Exception e) {
            log.error(
                "handle external view change event fail.",
                e
            );
        }
    }

    @Override
    public void updatePartitionNum(int partitionNum) {
        log.info("Helix controller is about to update partition number, task name: " + taskName);
        IdealState idealState = admin.getResourceIdealState(
            taskName,
            ZKRdsCdcClusterManager.DEFAULT_RESOURCE_NAME
        );
        int currentPartitionNum = idealState.getNumPartitions();

        if (currentPartitionNum == partitionNum) {
            log.warn(
                "Target partition number is the same as current partition number, will not update partition number, task name: " +
                    taskName);
            return;
        }

        BuffaloCheckpoint earliestCheckpoint = checkpointPersistenceProvider.getEarliestCheckpoint(currentPartitionNum);

        updatePartitionStorage(
            partitionNum,
            earliestCheckpoint
        );

        updateCheckpointNum(
            currentPartitionNum,
            partitionNum,
            earliestCheckpoint
        );

        updateIdealStatePartitionNum(
            idealState,
            partitionNum
        );
    }

    private void updatePartitionStorage(
        int partitionNum,
        BuffaloCheckpoint earliestCheckpoint
    ) {
        PartitionStorage partitionStorage = checkpointPersistenceProvider.buildPartitionStorage(rdsCdcSourceType, partitionNum, earliestCheckpoint);
        checkpointPersistenceProvider.storePartitionStorage(partitionStorage);
        log.info("Update partition storage success, partition number: " + partitionNum + ", earliest checkpoint: " +
                     earliestCheckpoint + ", task name: " + taskName);
    }

    private void updateIdealStatePartitionNum(
        IdealState idealState,
        int partitionNum
    ) {
        Map<String, List<String>> newInstanceLists = new HashMap<>();

        for (int i = 0; i < partitionNum; i++) {
            String key = ZKRdsCdcClusterManager.DEFAULT_RESOURCE_NAME + "_" + i;
            newInstanceLists.put(
                key,
                new ArrayList<>()
            );
        }

        idealState.setNumPartitions(partitionNum);
        idealState.setPreferenceLists(newInstanceLists);
        admin.setResourceIdealState(
            taskName,
            ZKRdsCdcClusterManager.DEFAULT_RESOURCE_NAME,
            idealState
        );
        log.info("Update ideal state partition number to " + partitionNum + " success, task name: " + taskName);
    }

    private void updateCheckpointNum(
        int from,
        int to,
        BuffaloCheckpoint earliestCheckpoint
    ) {

        if (from > to) {
            List<Integer> changePartitionIds = new ArrayList<>();
            for (int i = to; i < from; i++) {
                changePartitionIds.add(i);
            }

            checkpointPersistenceProvider.deleteCheckpoints(changePartitionIds);
        } else {
            for (int i = from; i < to; i++) {
                checkpointPersistenceProvider.storeCheckpointIfNotExists(
                    i,
                    earliestCheckpoint
                );
            }
        }
        log.info("Update checkpoint number from + " + from + " to " + to + " success.");
    }

    @Override
    public boolean isLeader() {
        return helixManager.isLeader();
    }
}
