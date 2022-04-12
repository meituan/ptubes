package com.meituan.ptubes.reader.monitor.collector;

import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.monitor.metrics.ReaderTaskStatMetrics;

public class ReaderTaskStatMetricsCollector extends AbstractStatMetricsCollector<ReaderTaskStatMetrics> {

    public ReaderTaskStatMetricsCollector(String readerTaskName) {
        this.data = new ReaderTaskStatMetrics(readerTaskName);
    }

    @Override
    public synchronized void initStatMetrics(ReaderTaskStatMetrics initData) {
        throw new UnsupportedOperationException("ReadTaskStatMetricsCollector unsupport init data");
    }

    @Override
    public synchronized void resetStatMetrics() {
        throw new UnsupportedOperationException("ReadTaskStatMetricsCollector unsupport reset data");
    }

    public void setReaderTaskName(String readerTaskName) {
        data.setReaderTaskName(readerTaskName);
    }
    public String getReaderTaskName() {
        return data.getReaderTaskName();
    }

    public void setReaderTaskStartTime(long startTime) {
        data.setReaderTaskStartTime(startTime);
    }
    public long getReaderTaskStartTime() {
        return data.getReaderTaskStartTime();
    }

    public void incrEventWriteCounter() {
        data.getEventWriteCounter().incrementAndGet();
    }
    public long getEventWriteCounter() {
        return data.getEventWriteCounter().get();
    }

    public void incrEventReadCounter() {
        data.getEventReadCounter().incrementAndGet();
    }
    public long getEventReadCounter() {
        return data.getEventReadCounter().get();
    }

    public void setLastEventBinlogTime(long binlogEventTime) {
        data.setLastEventBinlogTime(binlogEventTime);
    }
    public long getLastEventBinlogTime() {
        return data.getLastEventBinlogTime();
    }

    public void setLastEventDbToProducerCost(long dbToProducerCost) {
        data.setLastEventDbToProducerCost(dbToProducerCost);
    }
    public long getLastEventDbToProducerCost() {
        return data.getLastEventDbToProducerCost();
    }

    public void setLastEventProducerToStorageCost(long producerToStorageCost) {
        data.setLastEventProducerToStorageCost(producerToStorageCost);
    }
    public long getLastEventProducerToStorageCost() {
        return data.getLastEventProducerToStorageCost();
    }

    public void setLastEventStorageToReadChannelCost(long storageToReadChannelCost) {
        data.setLastEventStorageToReadChannelCost(storageToReadChannelCost);
    }
    public long getLastEventStorageToReadChannelCost() {
        return data.getLastEventStorageToReadChannelCost();
    }

    public void setLastHeartBeatTime(long heartBeatTime) {
        data.setLastHeartBeatTime(heartBeatTime);
    }
    public long getLastHeartBeatTime() {
        return data.getLastHeartBeatTime();
    }

    public void setHeartbeatRecieveTimestamp(long heartbeatRecieveTimestamp) {
        data.setHeartbeatRecieveTimestamp(heartbeatRecieveTimestamp);
    }
    public long getHeartbeatRecieveTimestamp() {
        return data.getHeartbeatRecieveTimestamp();
    }

    public void setLastHeartBeatBinlogInfo(BinlogInfo lastHeartBeatBinlogInfo) {
        data.setLastHeartBeatBinlogInfo(lastHeartBeatBinlogInfo);
    }
    public BinlogInfo getLastHeartBeatBinlogInfo() {
        return data.getLastHeartBeatBinlogInfo();
    }

    public void setStorageMode(StorageConstant.StorageMode storageMode) {
        data.setStorageMode(storageMode);
    }
    public StorageConstant.StorageMode getStorageMode() {
        return data.getStorageMode();
    }

    public void setMemStorageMinBinlogInfo(BinlogInfo memStorageMinBinlogInfo) {
        data.getMemStorageStatMetrics().setMinBinlogInfo(memStorageMinBinlogInfo);
    }
    public BinlogInfo getMemStorageMinBinlogInfo() {
        return data.getMemStorageStatMetrics().getMinBinlogInfo();
    }

    public void setMemStorageMaxBinlogInfo(BinlogInfo memStorageMaxBinlogInfo) {
        data.getMemStorageStatMetrics().setMaxBinlogInfo(memStorageMaxBinlogInfo);
    }
    public BinlogInfo getMemStorageMaxBinlogInfo() {
        return data.getMemStorageStatMetrics().getMaxBinlogInfo();
    }

    public void incrMemStorageEventReadCounter() {
        data.getMemStorageStatMetrics().getStorageEngineEventReadCounter().incrementAndGet();
    }
    public long getMemStorageEventReadCounter() {
        return data.getMemStorageStatMetrics().getStorageEngineEventReadCounter().get();
    }

    public void setFileStorageMinBinlogInfo(BinlogInfo fileStorageMinBinlogInfo) {
        data.getFileStorageStatMetrics().setMinBinlogInfo(fileStorageMinBinlogInfo);
    }
    public BinlogInfo getFileStorageMinBinlogInfo() {
        return data.getFileStorageStatMetrics().getMinBinlogInfo();
    }

    public void setFileStorageMaxBinlogInfo(BinlogInfo fileStorageMaxBinlogInfo) {
        data.getFileStorageStatMetrics().setMaxBinlogInfo(fileStorageMaxBinlogInfo);
    }
    public BinlogInfo getFileStorageMaxBinlogInfo() {
        return data.getFileStorageStatMetrics().getMaxBinlogInfo();
    }

    public void incrFileStorageEventReadCounter() {
        data.getFileStorageStatMetrics().getStorageEngineEventReadCounter().incrementAndGet();
    }
    public long getFileStorageEventReadCounter() {
        return data.getFileStorageStatMetrics().getStorageEngineEventReadCounter().get();
    }

}
