package com.meituan.ptubes.reader.monitor.metrics;

import java.util.concurrent.atomic.AtomicLong;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;

public class StroageEngineStatMetrics {

    private volatile BinlogInfo minBinlogInfo = new MySQLBinlogInfo();
    private volatile BinlogInfo maxBinlogInfo = new MySQLBinlogInfo();
    private AtomicLong storageEngineEventReadCounter = new AtomicLong(0);

    public StroageEngineStatMetrics() {
    }

    public BinlogInfo getMinBinlogInfo() {
        return minBinlogInfo;
    }

    public void setMinBinlogInfo(BinlogInfo minBinlogInfo) {
        this.minBinlogInfo = minBinlogInfo;
    }

    public BinlogInfo getMaxBinlogInfo() {
        return maxBinlogInfo;
    }

    public void setMaxBinlogInfo(BinlogInfo maxBinlogInfo) {
        this.maxBinlogInfo = maxBinlogInfo;
    }

    public AtomicLong getStorageEngineEventReadCounter() {
        return storageEngineEventReadCounter;
    }

    public void setStorageEngineEventReadCounter(AtomicLong storageEngineEventReadCounter) {
        this.storageEngineEventReadCounter = storageEngineEventReadCounter;
    }
}
