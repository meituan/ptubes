package com.meituan.ptubes.sdk.checkpoint;

public abstract class BuffaloCheckpoint implements Comparable<BuffaloCheckpoint> {
    protected long versionTs;

    protected CheckpointMode checkpointMode;

    public long getVersionTs() {
        return versionTs;
    }

    public void setVersionTs(long versionTs) {
        this.versionTs = versionTs;
    }

    public CheckpointMode getCheckpointMode() {
        return checkpointMode;
    }

    public void setCheckpointMode(CheckpointMode checkpointMode) {
        this.checkpointMode = checkpointMode;
    }

    public enum CheckpointMode {
        EARLIEST("earliest"),
        LATEST("latest"),
        NORMAL("normal");

        private String desc;

        CheckpointMode(String desc) {
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }
    }
}
