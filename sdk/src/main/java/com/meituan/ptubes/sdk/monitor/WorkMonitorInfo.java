package com.meituan.ptubes.sdk.monitor;

import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;

public class WorkMonitorInfo<C extends BuffaloCheckpoint> {
    private long heartbeatTimestamp;
    private C latestCheckpoint;
    private C heartbeatCheckpoint;

    public long getHeartbeatTimestamp() {
        return heartbeatTimestamp;
    }

    public void setHeartbeatTimestamp(long heartbeatTimestamp) {
        this.heartbeatTimestamp = heartbeatTimestamp;
    }

    public C getLatestCheckpoint() {
        return latestCheckpoint;
    }

    public void setLatestCheckpoint(C latestCheckpoint) {
        this.latestCheckpoint = latestCheckpoint;
    }

    public C getHeartbeatCheckpoint() {
        return heartbeatCheckpoint;
    }

    public void setHeartbeatCheckpoint(C heartbeatCheckpoint) {
        this.heartbeatCheckpoint = heartbeatCheckpoint;
    }

    @Override
    public String toString() {
        return "WorkMonitorInfo{" +
                "heartbeatTimestamp=" + heartbeatTimestamp +
                ", latestCheckpoint=" + latestCheckpoint +
                ", heartbeatCheckpoint=" + heartbeatCheckpoint +
                '}';
    }
}
