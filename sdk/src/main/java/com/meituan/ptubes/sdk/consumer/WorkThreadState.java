package com.meituan.ptubes.sdk.consumer;

import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.RdsCdcCheckpointFactory;
import java.util.LinkedList;
import com.meituan.ptubes.sdk.config.PtubesSdkConsumerConfig;
import com.meituan.ptubes.sdk.protocol.RdsPacket;

public class WorkThreadState {

    private String taskName;
    private int batchSize;
    private long heartbeatTimestamp;
    private long lastZKStoreTime;

    private StateId stateId;
    private PtubesSdkConsumerConfig.FailureMode failureMode;
    private BuffaloCheckpoint checkpoint;
    private BuffaloCheckpoint heartbeatCheckpoint;

    private LinkedList<RdsPacket.RdsEvent> rdsEvents = new LinkedList<>();

    public WorkThreadState(
        String taskName
    ) {
        this.taskName = taskName;
        this.lastZKStoreTime = 0;
        this.stateId = StateId.START;
    }

    public enum StateId {
        START,
        POLL_EVENTS,
        START_CONSUME,
        WAIT_FOR_ACK,
        CONSUME_SUCCESS,
        STORE_CHECKPOINT,
        STORE_CHECKPOINT_SUCCESS
    }

    public PtubesSdkConsumerConfig.FailureMode getFailureMode() {
        return failureMode;
    }

    public void setFailureMode(PtubesSdkConsumerConfig.FailureMode failureMode) {
        this.failureMode = failureMode;
    }

    public long getLastZKStoreTime() {
        return lastZKStoreTime;
    }

    public void setLastZKStoreTime(long lastZKStoreTime) {
        this.lastZKStoreTime = lastZKStoreTime;
    }

    public BuffaloCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(RdsPacket.Checkpoint checkpoint) {
        this.checkpoint = RdsCdcCheckpointFactory.buildFromPBCheckpoint(taskName, checkpoint);
    }

    public StateId getStateId() {
        return stateId;
    }

    public void setStateId(StateId stateId) {
        this.stateId = stateId;
    }

    public LinkedList<RdsPacket.RdsEvent> getRdsEvents() {
        return rdsEvents;
    }

    public void addRdsEvent(RdsPacket.RdsEvent rdsEvent) {
        rdsEvents.add(rdsEvent);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getHeartbeatTimestamp() {
        return heartbeatTimestamp;
    }

    public void setHeartbeatTimestamp(long heartbeatTimestamp) {
        this.heartbeatTimestamp = heartbeatTimestamp;
    }

    public BuffaloCheckpoint getHeartbeatCheckpoint() {
        return heartbeatCheckpoint;
    }

    public void setHeartbeatCheckpoint(BuffaloCheckpoint heartbeatCheckpoint) {
        this.heartbeatCheckpoint = heartbeatCheckpoint;
    }
}
