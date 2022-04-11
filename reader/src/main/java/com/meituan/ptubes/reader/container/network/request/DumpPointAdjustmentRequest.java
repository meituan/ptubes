package com.meituan.ptubes.reader.container.network.request;

public class DumpPointAdjustmentRequest {

    private String readerTask;
    private AdjustStrategy strategy;
    private long timestamp = -1;
    private int binlogNum = -1;
    private int binlogOffset = -1;

    public DumpPointAdjustmentRequest() {
    }

    public String getReaderTask() {
        return readerTask;
    }

    public void setReaderTask(String readerTask) {
        this.readerTask = readerTask;
    }

    public AdjustStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(AdjustStrategy strategy) {
        this.strategy = strategy;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getBinlogNum() {
        return binlogNum;
    }

    public void setBinlogNum(int binlogNum) {
        this.binlogNum = binlogNum;
    }

    public int getBinlogOffset() {
        return binlogOffset;
    }

    public void setBinlogOffset(int binlogOffset) {
        this.binlogOffset = binlogOffset;
    }

    public enum AdjustStrategy {
        FREEZE,
        TIMESTAMP,
        BINLOG_POSITION;
    }
}
