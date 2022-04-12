package com.meituan.ptubes.sdk.checkpoint;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.meituan.ptubes.sdk.protocol.RdsPacket.Checkpoint;

public class RdsCdcCheckpointFactory {

    private static Map<String, Long> checkpointVersionMap = new ConcurrentHashMap<>();
    /**
     * mysql
     * @param taskName
     * @param checkpoint
     * @return
     */
    public static synchronized MysqlCheckpoint buildFromPBCheckpoint(String taskName, Checkpoint checkpoint) {
        MysqlCheckpoint rdsCdcCheckpoint = new MysqlCheckpoint();
        long checkpointVersion = checkpointVersionMap.getOrDefault(taskName, -1L);

        rdsCdcCheckpoint.setServerId(checkpoint.getServerId());
        rdsCdcCheckpoint.setBinlogFile(checkpoint.getBinlogFile());
        rdsCdcCheckpoint.setBinlogOffset(checkpoint.getBinlogOffset());
        rdsCdcCheckpoint.setUuid(checkpoint.getUuid());
        rdsCdcCheckpoint.setTransactionId(checkpoint.getTransactionId());
        rdsCdcCheckpoint.setEventIndex(checkpoint.getEventIndex());
        rdsCdcCheckpoint.setTimestamp(checkpoint.getTimestamp());
        rdsCdcCheckpoint.setCheckpointMode(BuffaloCheckpoint.CheckpointMode.NORMAL);
        rdsCdcCheckpoint.setVersionTs(checkpointVersion);

        return rdsCdcCheckpoint;
    }

    public static synchronized BuffaloCheckpoint buildFromLionCheckpoint(String taskName, BuffaloCheckpoint checkpoint) {
        long checkpointVersion = checkpointVersionMap.getOrDefault(taskName, -1L);
        checkpointVersion = Math.max(checkpointVersion, checkpoint.getVersionTs());
        checkpointVersionMap.put(taskName, checkpointVersion);

        return checkpoint;
    }

    public static synchronized void shutdownTask(String taskName) {
        checkpointVersionMap.remove(taskName);
    }
}
