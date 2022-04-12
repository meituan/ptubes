package com.meituan.ptubes.sdk.config;

import com.meituan.ptubes.sdk.AckType;

import java.util.Objects;

public class PtubesSdkConsumerConfig {

    // Processing mode after business callback fails
    private String failureMode;

    // The number of retries after the business callback fails, -1 means retries all the time
    private int retryTimes;

    // The time interval between the storage point and the router service synchronization
    private int checkpointSyncIntervalMs;

    // Instantiate different IRdsCdcEventListener according to the options, now there are two modes of single and batch
    private String consumptionMode;

    // In batch consumption mode, the number of events per batch
    private int batchConsumeSize;

    // In batch consumption mode, the waiting timeout time of each batch, -1 means waiting all the time
    private int batchConsumeTimeoutMs;

    // The timeout time of each callback business logic, if it exceeds the specified time, it means failure, -1 means blocking waiting
    private int workerTimeoutMs;

    // qps limit
    private int qpsLimit;

    // ack type (default null)
    private AckType ackType;

    public PtubesSdkConsumerConfig() {
        this.failureMode = "RETRY";
        this.retryTimes = -1;
        this.checkpointSyncIntervalMs = 120000;
        this.consumptionMode = "SINGLE";
        this.batchConsumeSize = 1;
        this.batchConsumeTimeoutMs = 6000;
        this.workerTimeoutMs = 6000;
        this.qpsLimit = 100;
    }


    public enum FailureMode {
        RETRY("retry"),
        SKIP("skip");

        private String desc;

        FailureMode(String desc) {
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }
    }

    public enum ConsumptionMode {
        SINGLE("single"),
        BATCH("batch");

        private String desc;

        ConsumptionMode(String desc) {
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PtubesSdkConsumerConfig that = (PtubesSdkConsumerConfig) o;
        return retryTimes == that.retryTimes &&
            checkpointSyncIntervalMs == that.checkpointSyncIntervalMs &&
            batchConsumeSize == that.batchConsumeSize &&
            batchConsumeTimeoutMs == that.batchConsumeTimeoutMs &&
            workerTimeoutMs == that.workerTimeoutMs &&
            qpsLimit == that.qpsLimit &&
            ackType == that.ackType &&
            Objects.equals(
                failureMode,
                that.failureMode
            ) &&
            Objects.equals(
                consumptionMode,
                that.consumptionMode
            );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            failureMode,
            retryTimes,
            checkpointSyncIntervalMs,
            consumptionMode,
            batchConsumeSize,
            batchConsumeTimeoutMs,
            workerTimeoutMs,
            qpsLimit,
            ackType
        );
    }

    public String getFailureMode() {
        return failureMode;
    }

    public void setFailureMode(String failureMode) {
        this.failureMode = failureMode;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getCheckpointSyncIntervalMs() {
        return checkpointSyncIntervalMs;
    }

    public void setCheckpointSyncIntervalMs(int checkpointSyncIntervalMs) {
        this.checkpointSyncIntervalMs = checkpointSyncIntervalMs;
    }

    public String getConsumptionMode() {
        return consumptionMode;
    }

    public void setConsumptionMode(String consumptionMode) {
        this.consumptionMode = consumptionMode;
    }

    public int getBatchConsumeSize() {
        return batchConsumeSize;
    }

    public void setBatchConsumeSize(int batchConsumeSize) {
        this.batchConsumeSize = batchConsumeSize;
    }

    public int getBatchConsumeTimeoutMs() {
        return batchConsumeTimeoutMs;
    }

    public void setBatchConsumeTimeoutMs(int batchConsumeTimeoutMs) {
        this.batchConsumeTimeoutMs = batchConsumeTimeoutMs;
    }

    public int getWorkerTimeoutMs() {
        return workerTimeoutMs;
    }

    public void setWorkerTimeoutMs(int workerTimeoutMs) {
        this.workerTimeoutMs = workerTimeoutMs;
    }

    public int getQpsLimit() {
        return qpsLimit;
    }

    public void setQpsLimit(int qpsLimit) {
        this.qpsLimit = qpsLimit;
    }

    public AckType getAckType() {
        return ackType;
    }

    public void setAckType(AckType ackType) {
        this.ackType = ackType;
    }

    @Override
    public String toString() {
        return "RdsCdcClientConsumerConfig{" +
                "failureMode='" + failureMode + '\'' +
                ", retryTimes=" + retryTimes +
                ", checkpointSyncIntervalMs=" + checkpointSyncIntervalMs +
                ", consumptionMode='" + consumptionMode + '\'' +
                ", batchConsumeSize=" + batchConsumeSize +
                ", batchConsumeTimeoutMs=" + batchConsumeTimeoutMs +
                ", workerTimeoutMs=" + workerTimeoutMs +
                ", qpsLimit=" + qpsLimit +
                ", ackType=" + ackType +
                '}';
    }
}
