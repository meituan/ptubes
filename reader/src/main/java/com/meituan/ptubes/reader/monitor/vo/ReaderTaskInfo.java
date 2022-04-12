package com.meituan.ptubes.reader.monitor.vo;

import java.util.Map;

public class ReaderTaskInfo {

    private String readerTaskName;
    private long readerTaskStartTime;

    private long freqWriteEvents;
    private long freqReadEvents;
    private long timestampEventReceive;
    private long latencyEventReceive;
    private long latencyWriteEvent;
    private long latencyReadEvent;

    private long heartbeatTimestamp;
    private long heartbeatRecieveTimestamp;
    private Map<String, Object> heartbeatBinlogInfo;

    private String storageMode;
    private StorageEngineDetail memStorageEngineDetail;
    private StorageEngineDetail fileStorageEngineDetail;

    public ReaderTaskInfo(
        String readerTaskName,
        long readerTaskStartTime,
        long freqWriteEvents,
        long freqReadEvents,
        long timestampEventReceive,
        long latencyEventReceive,
        long latencyWriteEvent,
        long latencyReadEvent,
        long heartbeatTimestamp,
        long heartbeatRecieveTimestamp,
        Map<String, Object> heartbeatBinlogInfo,
        String storageMode,
        StorageEngineDetail memStorageEngineDetail,
        StorageEngineDetail fileStorageEngineDetail
    ) {
        this.readerTaskName = readerTaskName;
        this.readerTaskStartTime = readerTaskStartTime;
        this.freqWriteEvents = freqWriteEvents;
        this.freqReadEvents = freqReadEvents;
        this.timestampEventReceive = timestampEventReceive;
        this.latencyEventReceive = latencyEventReceive;
        this.latencyWriteEvent = latencyWriteEvent;
        this.latencyReadEvent = latencyReadEvent;
        this.heartbeatTimestamp = heartbeatTimestamp;
        this.heartbeatRecieveTimestamp = heartbeatRecieveTimestamp;
        this.heartbeatBinlogInfo = heartbeatBinlogInfo;
        this.storageMode = storageMode;
        this.memStorageEngineDetail = memStorageEngineDetail;
        this.fileStorageEngineDetail = fileStorageEngineDetail;
    }

    public String getReaderTaskName() {
        return readerTaskName;
    }

    public void setReaderTaskName(String readerTaskName) {
        this.readerTaskName = readerTaskName;
    }

    public long getReaderTaskStartTime() {
        return readerTaskStartTime;
    }

    public void setReaderTaskStartTime(long readerTaskStartTime) {
        this.readerTaskStartTime = readerTaskStartTime;
    }

    public long getFreqWriteEvents() {
        return freqWriteEvents;
    }

    public void setFreqWriteEvents(long freqWriteEvents) {
        this.freqWriteEvents = freqWriteEvents;
    }

    public long getFreqReadEvents() {
        return freqReadEvents;
    }

    public void setFreqReadEvents(long freqReadEvents) {
        this.freqReadEvents = freqReadEvents;
    }

    public long getTimestampEventReceive() {
        return timestampEventReceive;
    }

    public void setTimestampEventReceive(long timestampEventReceive) {
        this.timestampEventReceive = timestampEventReceive;
    }

    public long getLatencyEventReceive() {
        return latencyEventReceive;
    }

    public void setLatencyEventReceive(long latencyEventReceive) {
        this.latencyEventReceive = latencyEventReceive;
    }

    public long getLatencyWriteEvent() {
        return latencyWriteEvent;
    }

    public void setLatencyWriteEvent(long latencyWriteEvent) {
        this.latencyWriteEvent = latencyWriteEvent;
    }

    public long getLatencyReadEvent() {
        return latencyReadEvent;
    }

    public void setLatencyReadEvent(long latencyReadEvent) {
        this.latencyReadEvent = latencyReadEvent;
    }

    public long getHeartbeatTimestamp() {
        return heartbeatTimestamp;
    }

    public void setHeartbeatTimestamp(long heartbeatTimestamp) {
        this.heartbeatTimestamp = heartbeatTimestamp;
    }

    public long getHeartbeatRecieveTimestamp() {
        return heartbeatRecieveTimestamp;
    }

    public void setHeartbeatRecieveTimestamp(long heartbeatRecieveTimestamp) {
        this.heartbeatRecieveTimestamp = heartbeatRecieveTimestamp;
    }

    public Map<String, Object> getHeartbeatBinlogInfo() {
        return heartbeatBinlogInfo;
    }

    public void setHeartbeatBinlogInfo(Map<String, Object> heartbeatBinlogInfo) {
        this.heartbeatBinlogInfo = heartbeatBinlogInfo;
    }

    public String getStorageMode() {
        return storageMode;
    }

    public void setStorageMode(String storageMode) {
        this.storageMode = storageMode;
    }

    public StorageEngineDetail getMemStorageEngineDetail() {
        return memStorageEngineDetail;
    }

    public void setMemStorageEngineDetail(StorageEngineDetail memStorageEngineDetail) {
        this.memStorageEngineDetail = memStorageEngineDetail;
    }

    public StorageEngineDetail getFileStorageEngineDetail() {
        return fileStorageEngineDetail;
    }

    public void setFileStorageEngineDetail(StorageEngineDetail fileStorageEngineDetail) {
        this.fileStorageEngineDetail = fileStorageEngineDetail;
    }

    public static class StorageEngineDetail {
        private Map<String, Object> minBinlogInfo;
        private Map<String, Object> maxBinlogInfo;
        private long readCount;

        public StorageEngineDetail() {
        }

        public StorageEngineDetail(
            Map<String, Object> minBinlogInfo,
            Map<String, Object> maxBinlogInfo,
            long readCount
        ) {
            this.minBinlogInfo = minBinlogInfo;
            this.maxBinlogInfo = maxBinlogInfo;
            this.readCount = readCount;
        }

        public Map<String, Object> getMinBinlogInfo() {
            return minBinlogInfo;
        }

        public void setMinBinlogInfo(Map<String, Object> minBinlogInfo) {
            this.minBinlogInfo = minBinlogInfo;
        }

        public Map<String, Object> getMaxBinlogInfo() {
            return maxBinlogInfo;
        }

        public void setMaxBinlogInfo(Map<String, Object> maxBinlogInfo) {
            this.maxBinlogInfo = maxBinlogInfo;
        }

        public long getReadCount() {
            return readCount;
        }

        public void setReadCount(long readCount) {
            this.readCount = readCount;
        }
    }


}
