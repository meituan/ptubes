package com.meituan.ptubes.sdk.model;



public class DataRequest {
    private int batchSize;
    private int timeoutMs;
    private int maxByteSize;

    public DataRequest() {

    }

    public DataRequest(
        int batchSize,
        int timeoutMs,
        int maxByteSize
    ) {
        this.batchSize = batchSize;
        this.timeoutMs = timeoutMs;
        this.maxByteSize = maxByteSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public int getMaxByteSize() {
        return maxByteSize;
    }

    public void setMaxByteSize(int maxByteSize) {
        this.maxByteSize = maxByteSize;
    }
}
