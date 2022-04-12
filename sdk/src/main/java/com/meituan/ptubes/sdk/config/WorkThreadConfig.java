package com.meituan.ptubes.sdk.config;


import com.meituan.ptubes.sdk.AckType;

public class WorkThreadConfig {

    private String taskName;
    private RdsCdcSourceType sourceType;
    private AckType ackType;
    private int checkpointSyncIntervalMs;
    private PtubesSdkConsumerConfig.ConsumptionMode consumptionMode;
    private int batchSize;
    private int batchTimeoutMs;
    private int workerTimeoutMs;
    private PtubesSdkConsumerConfig.FailureMode failureMode;
    private int retryTimes;
    private int qpsLimit;

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public AckType getAckType() {
        return ackType;
    }

    public void setAckType(AckType ackType) {
        this.ackType = ackType;
    }

    public RdsCdcSourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(RdsCdcSourceType sourceType) {
        this.sourceType = sourceType;
    }

    public int getCheckpointSyncIntervalMs() {
        return checkpointSyncIntervalMs;
    }

    public void setCheckpointSyncIntervalMs(int checkpointSyncIntervalMs) {
        this.checkpointSyncIntervalMs = checkpointSyncIntervalMs;
    }

    public PtubesSdkConsumerConfig.ConsumptionMode getConsumptionMode() {
        return consumptionMode;
    }

    public void setConsumptionMode(PtubesSdkConsumerConfig.ConsumptionMode consumptionMode) {
        this.consumptionMode = consumptionMode;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchTimeoutMs() {
        return batchTimeoutMs;
    }

    public void setBatchTimeoutMs(int batchTimeoutMs) {
        this.batchTimeoutMs = batchTimeoutMs;
    }

    public int getWorkerTimeoutMs() {
        return workerTimeoutMs;
    }

    public void setWorkerTimeoutMs(int workerTimeoutMs) {
        this.workerTimeoutMs = workerTimeoutMs;
    }

    public PtubesSdkConsumerConfig.FailureMode getFailureMode() {
        return failureMode;
    }

    public void setFailureMode(PtubesSdkConsumerConfig.FailureMode failureMode) {
        this.failureMode = failureMode;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getQpsLimit() {
        return qpsLimit;
    }

    public void setQpsLimit(int qpsLimit) {
        this.qpsLimit = qpsLimit;
    }
}
