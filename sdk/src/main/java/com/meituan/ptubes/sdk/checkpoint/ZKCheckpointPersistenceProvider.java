package com.meituan.ptubes.sdk.checkpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.JacksonUtil;
import com.meituan.ptubes.sdk.cluster.zk.ZKRdsCdcClusterManager;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;


public class ZKCheckpointPersistenceProvider implements ICheckpointPersistenceProvider {
    private final Logger log;
    private static final String KEY_CHECKPOINT = "checkpoint";

    private String taskName;
    private final RdsCdcSourceType sourceType;
    private HelixManager helixManager;
    private HelixPropertyStore<ZNRecord> propertyStore;
    private HelixPropertyStoreWrapper propertyStoreWrapper;

    ZKCheckpointPersistenceProvider(String taskName, String zkAddr, RdsCdcSourceType sourceType) throws Exception {
        this.taskName = taskName;
        this.sourceType = sourceType;
        this.helixManager = HelixManagerFactory.getZKHelixManager(
            taskName, ZKCheckpointPersistenceProvider.class.getName(), InstanceType.SPECTATOR, zkAddr
        );
        this.helixManager.connect();
        this.propertyStore = helixManager.getHelixPropertyStore();
        log = LoggerFactory.getLoggerByTask(ZKCheckpointPersistenceProvider.class, taskName);
        this.propertyStoreWrapper = new HelixPropertyStoreWrapper(propertyStore, log);
    }

    @Override
    public synchronized void storeCheckpointIfNotExists(int partitionId, BuffaloCheckpoint checkpoint) {
        if (checkpoint == null) {
            return;
        }

        String key = makeKeyWithTaskName(partitionId);
        if (propertyStoreWrapper.exists(key, AccessOption.PERSISTENT)) {
            return;
        }

        ZNRecord zn = new ZNRecord(Integer.toString(partitionId));
        zn.setSimpleField(KEY_CHECKPOINT, JacksonUtil.toJson(checkpoint));
        propertyStore.set(key, zn, AccessOption.PERSISTENT);
    }

    @Override
    public void storeCheckpointIfExists(int partitionId, BuffaloCheckpoint checkpoint) {
        if (checkpoint == null) {
            return;
        }

        String key = makeKeyWithTaskName(partitionId);
        if (!propertyStoreWrapper.exists(key, AccessOption.PERSISTENT)) {
            return;
        }

        ZNRecord zn = new ZNRecord(Integer.toString(partitionId));
        zn.setSimpleField(KEY_CHECKPOINT, JacksonUtil.toJson(checkpoint));
        propertyStore.set(key, zn, AccessOption.PERSISTENT);
    }

    @Override
    public synchronized BuffaloCheckpoint loadCheckpoint(int partitionId) {
        String key = makeKeyWithTaskName(partitionId);
        ZNRecord zn = propertyStore.get(key, null, AccessOption.PERSISTENT);
        if (zn == null) {
            log.error("fail to get ZNRecord for: " + key);
            return null;
        }

        String json = zn.getSimpleField(KEY_CHECKPOINT);
        BuffaloCheckpoint checkpoint = null;
        if (null == sourceType || RdsCdcSourceType.MYSQL == sourceType) {
            checkpoint = JacksonUtil.fromJson(json, MysqlCheckpoint.class);
        } else {
            throw new RuntimeException("unknown sourceType when load" + json);
        }
        return checkpoint;
    }

    @Override
    public synchronized void deleteCheckpoint(int partitionId) {
        String key = makeKeyWithTaskName(partitionId);

        if (propertyStoreWrapper.exists(key, AccessOption.PERSISTENT)) {
            propertyStore.remove(key, AccessOption.PERSISTENT);
        }
    }

    @Override
    public synchronized void deleteCheckpoints(List<Integer> partitionIds) {
        List<String> partitionKeys = new ArrayList<>();
        for (int partitionId : partitionIds) {
            partitionKeys.add(makeKeyWithTaskName(partitionId));
        }

        propertyStore.remove(partitionKeys, AccessOption.PERSISTENT);
    }

    @Override
    public synchronized void storePartitionStorage(PartitionStorage partitionStorage) {
        ZNRecord zn = new ZNRecord(ZKRdsCdcClusterManager.PARTITION_STORAGE_KEY);
        zn.setSimpleField(ZKRdsCdcClusterManager.PARTITION_STORAGE_KEY, JacksonUtil.toJson(partitionStorage));

        propertyStore.set("/" + ZKRdsCdcClusterManager.PARTITION_STORAGE_KEY, zn, AccessOption.PERSISTENT);
    }

    @Override
    public synchronized PartitionStorage loadPartitionStorage() {
        if (!propertyStoreWrapper.exists("/" + ZKRdsCdcClusterManager.PARTITION_STORAGE_KEY, AccessOption.PERSISTENT)) {
            return null;
        }

        ZNRecord zn = propertyStore.get("/" + ZKRdsCdcClusterManager.PARTITION_STORAGE_KEY, null, AccessOption.PERSISTENT);
        String partitionStorageString = zn.getSimpleField(ZKRdsCdcClusterManager.PARTITION_STORAGE_KEY);
        PartitionStorage partitionStorage = JacksonUtil.fromJson(partitionStorageString, PartitionStorage.class);
        return partitionStorage;
    }

    @Override
    public PartitionStorage buildPartitionStorage(RdsCdcSourceType sourceType, int partitionNum, BuffaloCheckpoint checkpoint) {
        if (null == sourceType) {
            log.error("sourceType is null in buildPartitionStorage. checkpoint is " + checkpoint);
            return new PartitionStorage(partitionNum, checkpoint);
        }
        switch (sourceType) {
            case MYSQL:
            default:
                return new PartitionStorage(partitionNum, checkpoint);
        }
    }

    @Override
    public BuffaloCheckpoint getEarliestCheckpoint(int totalPartition) {
        TreeSet<BuffaloCheckpoint> checkpoints = new TreeSet<>();

        for (int i = 0; i < totalPartition; i++) {
            BuffaloCheckpoint checkpoint = this.loadCheckpoint(i);
            if (checkpoint != null) {
                checkpoints.add(checkpoint);
            }
        }

        if (checkpoints.size() == 0) {
            return null;
        }

        return checkpoints.first();
    }

    private String makeKeyWithTaskName(int partitionId) {
        StringBuilder k = new StringBuilder(50);
        k.append("/");
        k.append(partitionId);
        k.append("_");
        k.append(this.taskName);
        return k.toString();
    }

    @Override
    public void close() {
        if (helixManager != null) {
            helixManager.disconnect();
            helixManager = null;
        }
    }
}
