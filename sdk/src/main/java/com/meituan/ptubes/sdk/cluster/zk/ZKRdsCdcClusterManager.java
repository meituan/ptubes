package com.meituan.ptubes.sdk.cluster.zk;

import com.meituan.ptubes.common.exception.RdsCdcClusterException;
import com.meituan.ptubes.common.exception.RdsCdcRuntimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.CheckpointPersistenceProviderFactory;
import com.meituan.ptubes.sdk.checkpoint.ICheckpointPersistenceProvider;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterController;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterManager;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterMember;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterPartitionNotifier;
import java.util.UUID;
import com.meituan.ptubes.common.utils.HostUtil;
import com.meituan.ptubes.sdk.config.PtubesSdkSubscriptionConfig;
import com.meituan.ptubes.sdk.config.RdsCdcClusterConfig;
import com.meituan.ptubes.sdk.config.RdsCdcHelixInstanceConfig;
import com.meituan.ptubes.sdk.consumer.FetchThread;
import com.meituan.ptubes.sdk.consumer.WorkThreadCluster;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;

public class ZKRdsCdcClusterManager implements IRdsCdcClusterManager {

    private final Logger log;

    private static final int MAX_STATE_TRANSITION_THREAD_NUM = 3;

    private static final String HELIX_MANAGER_ZK_SESSION_TIMEOUT_KEY = "zk.session.timeout";
    private static final String HELIX_MANAGER_MAX_DISCONNECT_THRESHOLD = "helixmanager.maxDisconnectThreshold";

    private static final int HELIX_CLUSTER_READY_SLEEP_INTERVAL = 1000;
    private static final int HELIX_CLUSTER_READY_SLEEP_TIME = 10;

    public static final String DEFAULT_STATE_MODEL = "OnlineOffline";
    public static final String DEFAULT_RESOURCE_NAME = "default-resource";
    public static final String PARTITION_STORAGE_KEY = "partition-storage";

    private FetchThread fetchThread;
    private IRdsCdcClusterPartitionNotifier notifier;

    private final RdsCdcClusterConfig rdsCdcClusterConfig;
    private final ZkClient zkClient;
    private final HelixAdmin admin;
    private final String taskName;
    private final String zkAddr;
    private final int numPartitions;
    private final int zkConnectionTimeoutMs;
    private final int zkSessionTimeoutMs;

    private IRdsCdcClusterController controller;
    private IRdsCdcClusterMember member;

    public ZKRdsCdcClusterManager(
        RdsCdcClusterConfig config,
        FetchThread fetchThread,
        WorkThreadCluster workThreadCluster
    ) throws Exception {
        this.log = LoggerFactory.getLoggerByTask(
            ZKRdsCdcClusterManager.class,
            fetchThread.getTaskName()
        );

        this.fetchThread = fetchThread;

        rdsCdcClusterConfig = config;
        zkAddr = config.getZkAddr();
        taskName = config.getTaskName();
        numPartitions = (int) (config.getNumPartitions());

        zkSessionTimeoutMs = config.getZkSessionTimeoutMs();
        zkConnectionTimeoutMs = config.getZkConnectionTimeoutMs();

        updateHelixManagerZkSessionTimeout(config.getZkSessionTimeoutMs());
        updateHelixManagerMaxDisconnectThreshold(config.getMaxDisconnectThreshold());

        zkClient = new ZkClient(
            zkAddr,
            zkSessionTimeoutMs,
            zkConnectionTimeoutMs,
            new ZNRecordSerializer()
        );
        admin = new ZKHelixAdmin(zkClient);

        createCluster(config.getInitCheckpoint());

        this.notifier = new ZKRdsCdcClusterPartitionNotifier(
            fetchThread,
            workThreadCluster
        );

        controller = new ZKRdsCdcClusterController(config);
    }

    @Override
    public void start() throws Exception {
        this.member = initMember(
            fetchThread,
            HostUtil.getLocalHostName(),
            this.notifier
        );
        this.member.join();

        if (controller != null) {
            controller.startController();
        }
    }

    @Override
    public void shutdown() {
        if (member != null && member.isConnected()) {
            member.leave();
        }

        if (controller != null) {
            controller.stopController();
        }
        if (zkClient != null) {
            zkClient.close();
        }
    }

    private void createCluster(BuffaloCheckpoint checkpoint) throws Exception {
        boolean firstCreate = false;
        String root = "/" + taskName;

        try {
            if (!zkClient.exists(root) && admin.addCluster(
                taskName,
                false
            )) {
                firstCreate = true;
                admin.addStateModelDef(
                    taskName,
                    DEFAULT_STATE_MODEL,
                    new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline())
                );
                admin.addResource(
                    taskName,
                    DEFAULT_RESOURCE_NAME,
                    numPartitions,
                    DEFAULT_STATE_MODEL,
                    RebalanceMode.FULL_AUTO.toString()
                );
                admin.rebalance(
                    taskName,
                    DEFAULT_RESOURCE_NAME,
                    1
                );
            }
        } catch (Exception e) {
            String message = "Create cluster " + taskName + " failed.";
            log.error(
                message,
                e
            );
        }

        int clusterSleepTime = 0;
        while (getNumPartitionsInResource() != numPartitions) {
            if (clusterSleepTime >= HELIX_CLUSTER_READY_SLEEP_TIME) {
                String message = "Cluster structure is not ready for task: " + taskName;
                Exception e = new RdsCdcRuntimeException(message);
                throw e;
            }
            Thread.sleep(HELIX_CLUSTER_READY_SLEEP_INTERVAL);
            clusterSleepTime++;
        }

        CheckpointPersistenceProviderFactory.initPersistenceProvider(
            taskName,
            zkAddr,
            PtubesSdkSubscriptionConfig.RouterMode.ZOOKEEPER,
            rdsCdcClusterConfig.getRdsCdcSourceType()
        );

        if (firstCreate) {
            ICheckpointPersistenceProvider checkpointPersistenceProvider = CheckpointPersistenceProviderFactory.getInstance(taskName);

            for (int i = 0; i < numPartitions; i++) {
                checkpointPersistenceProvider.storeCheckpointIfNotExists(
                    i,
                    checkpoint
                );
            }

            checkpointPersistenceProvider.storePartitionStorage(
                    checkpointPersistenceProvider.buildPartitionStorage(
                            rdsCdcClusterConfig.getRdsCdcSourceType(),
                            numPartitions,
                            checkpoint
                    ));
        }
    }

    public IRdsCdcClusterMember initMember(
        FetchThread fetchThread,
        String hostname,
        IRdsCdcClusterPartitionNotifier notifier
    ) throws RdsCdcClusterException {
        String instanceName = hostname + "_" + UUID.randomUUID()
            .toString();
        if (admin == null) {
            throw new RdsCdcClusterException("HelixAdmin is NULL, can not init member");
        }
        try {
            InstanceConfig tempConfig = admin.getInstanceConfig(
                taskName,
                instanceName
            );

            admin.dropInstance(
                taskName,
                tempConfig
            );
            log.warn("Member id already exists! Overwriting instance for instance=" + instanceName);
        } catch (HelixException e) {
        }

        RdsCdcHelixInstanceConfig config = new RdsCdcHelixInstanceConfig(instanceName);
        config.setHostName(hostname);
        config.setInstanceEnabled(true);

        // Reduce the number of thread pools for Helix state migration callbacks, Helix defaults to 40 threads
        config.setStateTransitionMaxThread(MAX_STATE_TRANSITION_THREAD_NUM);

        admin.addInstance(
            taskName,
            config
        );

        IRdsCdcClusterMember member = new ZKRdsCdcClusterMember(
            instanceName,
            notifier,
                rdsCdcClusterConfig,
            fetchThread
        );

        Runtime.getRuntime()
            .addShutdownHook(new Thread() {
                @Override
                public void run() {
                    member.leave();
                }
            });

        return member;
    }

    private int getNumPartitionsInResource() {
        if (admin == null) {
            log.warn("Helix admin is null for task: " + taskName);
            return -1;
        }

        try {
            IdealState idealState = admin.getResourceIdealState(
                taskName,
                DEFAULT_RESOURCE_NAME
            );
            return idealState.getNumPartitions();
        } catch (Exception e) {
            log.warn(
                "Resource " + DEFAULT_RESOURCE_NAME + " not found in " + taskName,
                e
            );
            return 0;
        }
    }

    @Override
    public void updatePartitionNum(int partitionNum) {
        try {
            if (this.controller == null || !this.controller.isLeader()) {
                log.warn("Current controller is null or is not leader, will not update partition number");
                return;
            }

            this.controller.updatePartitionNum(partitionNum);
        } catch (Exception e) {
            String message = "Update partition number failed, task name: " + this.taskName;
            log.error(
                message,
                e
            );
        }
    }

    private void updateHelixManagerZkSessionTimeout(int timeoutMs) {
        System.setProperty(
            HELIX_MANAGER_ZK_SESSION_TIMEOUT_KEY,
            Integer.toString(timeoutMs)
        );
    }

    private void updateHelixManagerMaxDisconnectThreshold(int maxDisconnectThreshold) {
        System.setProperty(
            HELIX_MANAGER_MAX_DISCONNECT_THRESHOLD,
            Integer.toString(maxDisconnectThreshold)
        );
    }
}
