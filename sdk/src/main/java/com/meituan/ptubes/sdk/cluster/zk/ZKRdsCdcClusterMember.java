package com.meituan.ptubes.sdk.cluster.zk;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.sdk.checkpoint.CheckpointPersistenceProviderFactory;
import com.meituan.ptubes.sdk.checkpoint.ICheckpointPersistenceProvider;
import org.I0Itec.zkclient.IZkDataListener;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterMember;
import com.meituan.ptubes.sdk.cluster.IRdsCdcClusterPartitionNotifier;
import com.meituan.ptubes.sdk.config.RdsCdcClusterConfig;
import com.meituan.ptubes.sdk.consumer.FetchThread;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.HelixPropertyStore;

import static com.meituan.ptubes.sdk.cluster.zk.ZKRdsCdcClusterManager.DEFAULT_STATE_MODEL;
import static com.meituan.ptubes.sdk.cluster.zk.ZKRdsCdcClusterManager.PARTITION_STORAGE_KEY;



public class ZKRdsCdcClusterMember implements IRdsCdcClusterMember {

    private final Logger log;

    private final String id;
    private final String taskName;
    private final String zkAddr;

    private FetchThread fetchThread;
    private HelixManager manager;
    private HelixPropertyStore<ZNRecord> propertyStore;
    private ICheckpointPersistenceProvider checkpointPersistenceProvider;
    private IZkDataListener partitionStorageDataListener;

    public ZKRdsCdcClusterMember(
        String id,
        IRdsCdcClusterPartitionNotifier notifier,
        RdsCdcClusterConfig config,
        FetchThread fetchThread
    ) {
        this.log = LoggerFactory.getLoggerByTask(
            ZKRdsCdcClusterMember.class,
            config.getTaskName()
        );
        this.id = id;
        this.taskName = config.getTaskName();
        this.zkAddr = config.getZkAddr();

        this.fetchThread = fetchThread;

        this.manager = HelixManagerFactory.getZKHelixManager(
            taskName,
            id,
            InstanceType.PARTICIPANT,
            zkAddr
        );

        StateMachineEngine stateMach = manager.getStateMachineEngine();
        ZKRdsCdcClusterNotifierFactory modelFactory = new ZKRdsCdcClusterNotifierFactory(
            notifier,
            taskName
        );
        stateMach.registerStateModelFactory(
            DEFAULT_STATE_MODEL,
            modelFactory
        );

        try {
            this.checkpointPersistenceProvider = CheckpointPersistenceProviderFactory.getInstance(taskName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.partitionStorageDataListener = new ZKRdsCdcClusterPartitionNumListener(
            this.taskName,
            this.fetchThread,
            this.checkpointPersistenceProvider
        );
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void join() {
        try {
            manager.connect();
            this.propertyStore = manager.getHelixPropertyStore();
            this.propertyStore.subscribeDataChanges(
                "/" + PARTITION_STORAGE_KEY,
                this.partitionStorageDataListener
            );
        } catch (Exception e) {
            log.error("Member " + id + " could not connect! " + e);
        }
    }

    @Override
    public void leave() {
        while (true) {
            try {
                if (manager != null) {
                    manager.disconnect();
                }
                break;
            } catch (Exception e) {
                log.error("Member " + id + " could not disconnect now! Retry in 10s. " + e);
                try {
                    Thread.sleep(10000);
                } catch (Exception ex) {
                    log.warn(ex.getLocalizedMessage());
                }
            }
        }
    }

    @Override
    public boolean isConnected() {
        return manager.isConnected();
    }
}
