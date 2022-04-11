package com.meituan.ptubes.sdk.checkpoint;

import java.util.List;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;


public interface ICheckpointPersistenceProvider {
    /**
     * Persistent storage point
     */
    void storeCheckpointIfNotExists(int partitionId, BuffaloCheckpoint checkpoint);

    /**
     * Persistent storage point
     */
    void storeCheckpointIfExists(int partitionId, BuffaloCheckpoint checkpoint);

    /**
     * Query storage point
     */
    BuffaloCheckpoint loadCheckpoint(int partitionId);

    /**
     * Query the oldest storage point
     */
    BuffaloCheckpoint getEarliestCheckpoint(int totalPartition);

    /**
     * delete storage point
     */
    void deleteCheckpoint(int partitionId);

    /**
     * Batch delete storage points
     */
    void deleteCheckpoints(List<Integer> partitionIds);

    /**
     * When the shard changes, temporarily store the number of target shards and the oldest storage point
     */
    void storePartitionStorage(PartitionStorage partitionStorage);

    /**
     * Query the number of target shards and the oldest storage point, this structure is only used for shard change logic
     */
    PartitionStorage loadPartitionStorage();

    /**
     * Construct storage point information
     * Before version 1.5 is fully released, the sourceType field can only be null
     *
     * @param sourceType
     * @param partitionNum
     * @param checkpoint
     * @return
     */
    PartitionStorage buildPartitionStorage(RdsCdcSourceType sourceType, int partitionNum, BuffaloCheckpoint checkpoint);

    void close();
}
