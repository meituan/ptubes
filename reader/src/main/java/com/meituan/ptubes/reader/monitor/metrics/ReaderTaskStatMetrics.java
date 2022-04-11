package com.meituan.ptubes.reader.monitor.metrics;

import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import java.util.concurrent.atomic.AtomicLong;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;

public class ReaderTaskStatMetrics {

	private volatile String readerTaskName = "";
	private volatile long readerTaskStartTime = 0;

	private AtomicLong eventWriteCounter = new AtomicLong(0);
	private AtomicLong eventReadCounter = new AtomicLong(0);
	private volatile long lastEventBinlogTime = 0;						// binlogTime
	private volatile long lastEventDbToProducerCost = 0;						// db -> or
	private volatile long lastEventProducerToStorageCost = 0;					// or -> storage
	private volatile long lastEventStorageToReadChannelCost = 0;		// storage -> readChannel

	private volatile long lastHeartBeatTime = 0;
	private volatile long heartbeatRecieveTimestamp = 0L;
	private volatile BinlogInfo lastHeartBeatBinlogInfo = new MySQLBinlogInfo();

	/*storage involed*/
	private volatile StorageConstant.StorageMode storageMode = StorageConstant.StorageMode.MEM;
	private volatile StroageEngineStatMetrics memStorageStatMetrics = new StroageEngineStatMetrics();
	private volatile StroageEngineStatMetrics fileStorageStatMetrics = new StroageEngineStatMetrics();

	public ReaderTaskStatMetrics(String readerTaskName) {
		this.readerTaskName = readerTaskName;
		this.readerTaskStartTime = System.currentTimeMillis();
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

	public AtomicLong getEventWriteCounter() {
		return eventWriteCounter;
	}

	public void setEventWriteCounter(AtomicLong eventWriteCounter) {
		this.eventWriteCounter = eventWriteCounter;
	}

	public AtomicLong getEventReadCounter() {
		return eventReadCounter;
	}

	public void setEventReadCounter(AtomicLong eventReadCounter) {
		this.eventReadCounter = eventReadCounter;
	}

	public long getLastEventBinlogTime() {
		return lastEventBinlogTime;
	}

	public void setLastEventBinlogTime(long lastEventBinlogTime) {
		this.lastEventBinlogTime = lastEventBinlogTime;
	}

	public long getLastEventDbToProducerCost() {
		return lastEventDbToProducerCost;
	}

	public void setLastEventDbToProducerCost(long lastEventDbToProducerCost) {
		this.lastEventDbToProducerCost = lastEventDbToProducerCost;
	}

	public long getLastEventProducerToStorageCost() {
		return lastEventProducerToStorageCost;
	}

	public void setLastEventProducerToStorageCost(long lastEventProducerToStorageCost) {
		this.lastEventProducerToStorageCost = lastEventProducerToStorageCost;
	}

	public long getLastEventStorageToReadChannelCost() {
		return lastEventStorageToReadChannelCost;
	}

	public void setLastEventStorageToReadChannelCost(long lastEventStorageToReadChannelCost) {
		this.lastEventStorageToReadChannelCost = lastEventStorageToReadChannelCost;
	}

	public long getLastHeartBeatTime() {
		return lastHeartBeatTime;
	}

	public void setLastHeartBeatTime(long lastHeartBeatTime) {
		this.lastHeartBeatTime = lastHeartBeatTime;
	}

	public long getHeartbeatRecieveTimestamp() {
		return heartbeatRecieveTimestamp;
	}

	public void setHeartbeatRecieveTimestamp(long heartbeatRecieveTimestamp) {
		this.heartbeatRecieveTimestamp = heartbeatRecieveTimestamp;
	}

	public BinlogInfo getLastHeartBeatBinlogInfo() {
		return lastHeartBeatBinlogInfo;
	}

	public void setLastHeartBeatBinlogInfo(BinlogInfo lastHeartBeatBinlogInfo) {
		this.lastHeartBeatBinlogInfo = lastHeartBeatBinlogInfo;
	}

	public StorageConstant.StorageMode getStorageMode() {
		return storageMode;
	}

	public void setStorageMode(StorageConstant.StorageMode storageMode) {
		this.storageMode = storageMode;
	}

	public StroageEngineStatMetrics getMemStorageStatMetrics() {
		return memStorageStatMetrics;
	}

	public void setMemStorageStatMetrics(StroageEngineStatMetrics memStorageStatMetrics) {
		this.memStorageStatMetrics = memStorageStatMetrics;
	}

	public StroageEngineStatMetrics getFileStorageStatMetrics() {
		return fileStorageStatMetrics;
	}

	public void setFileStorageStatMetrics(StroageEngineStatMetrics fileStorageStatMetrics) {
		this.fileStorageStatMetrics = fileStorageStatMetrics;
	}
}
