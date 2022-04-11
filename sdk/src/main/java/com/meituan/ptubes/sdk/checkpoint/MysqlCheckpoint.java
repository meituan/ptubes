package com.meituan.ptubes.sdk.checkpoint;

import java.util.Objects;

public class MysqlCheckpoint extends BuffaloCheckpoint {
    private String uuid;
    private long transactionId;
    private long eventIndex;
    private int serverId;
    private int binlogFile;
    private long binlogOffset;
    private long timestamp;

    public MysqlCheckpoint() {
        super();
    }

    public MysqlCheckpoint(long timestamp) {
        super();
        this.versionTs = System.currentTimeMillis();
        this.checkpointMode = CheckpointMode.LATEST;
        this.uuid = "00000000-0000-0000-0000-000000000000";
        this.transactionId = -1L;
        this.eventIndex = -1L;
        this.serverId = -1;
        this.binlogFile = -1;
        this.binlogOffset = -1L;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(BuffaloCheckpoint o) {
        if (o == null) {
            return -1;
        }
        if (!(o instanceof MysqlCheckpoint)) {
            throw new RuntimeException("incorrect type");
        }
        MysqlCheckpoint that = (MysqlCheckpoint) o;

        if (this.versionTs != that.versionTs) {
            return Long.compare(that.versionTs, this.versionTs);
        }

        if (this.getCheckpointMode() != CheckpointMode.NORMAL && that.getCheckpointMode() != CheckpointMode.NORMAL) {
            return 0;
        }

        if (this.getCheckpointMode() == CheckpointMode.NORMAL && that.getCheckpointMode() != CheckpointMode.NORMAL) {
            return -1;
        }

        if (this.getCheckpointMode() != CheckpointMode.NORMAL && that.getCheckpointMode() == CheckpointMode.NORMAL) {
            return 1;
        }

        if (this.timestamp != that.timestamp) {
            return Long.compare(this.timestamp, that.timestamp);
        }

        if (this.serverId != that.serverId) {
            return 0;
        }

        if (this.binlogFile != that.binlogFile) {
            return this.binlogFile - that.binlogFile;
        }

        if (this.binlogOffset != that.binlogOffset) {
            return Long.compare(this.binlogOffset, that.binlogOffset);
        }

        return Long.compare(this.eventIndex, that.eventIndex);
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public long getEventIndex() {
        return eventIndex;
    }

    public void setEventIndex(long eventIndex) {
        this.eventIndex = eventIndex;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public int getBinlogFile() {
        return binlogFile;
    }

    public void setBinlogFile(int binlogFile) {
        this.binlogFile = binlogFile;
    }

    public long getBinlogOffset() {
        return binlogOffset;
    }

    public void setBinlogOffset(long binlogOffset) {
        this.binlogOffset = binlogOffset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MysqlCheckpoint that = (MysqlCheckpoint) o;
        return transactionId == that.transactionId &&
            eventIndex == that.eventIndex &&
            serverId == that.serverId &&
            binlogFile == that.binlogFile &&
            binlogOffset == that.binlogOffset &&
            timestamp == that.timestamp &&
            versionTs == that.versionTs &&
            Objects.equals(
                uuid,
                that.uuid
            ) &&
            checkpointMode == that.checkpointMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            uuid,
            transactionId,
            eventIndex,
            serverId,
            binlogFile,
            binlogOffset,
            timestamp,
            versionTs,
            checkpointMode
        );
    }

    @Override
    public String toString() {
        return "RdsCdcMysqlCheckpoint{" +
            "uuid='" + uuid + '\'' +
            ", transactionId=" + transactionId +
            ", eventIndex=" + eventIndex +
            ", serverId=" + serverId +
            ", binlogFile=" + binlogFile +
            ", binlogOffset=" + binlogOffset +
            ", timestamp=" + timestamp +
            ", versionTs=" + versionTs +
            ", checkpointMode=" + checkpointMode +
            '}';
    }
}
