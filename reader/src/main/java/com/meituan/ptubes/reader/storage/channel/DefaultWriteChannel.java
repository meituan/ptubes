package com.meituan.ptubes.reader.storage.channel;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import java.util.concurrent.locks.ReentrantLock;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.manager.write.FileWriteManager;
import com.meituan.ptubes.reader.storage.manager.write.MemWriteManager;
import com.meituan.ptubes.reader.storage.manager.write.MixWriteManager;
import com.meituan.ptubes.reader.storage.manager.write.WriteManager;
import org.apache.commons.lang3.tuple.Pair;


public class DefaultWriteChannel extends AbstractLifeCycle implements WriteChannel {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultWriteChannel.class);

	private final SourceType sourceType;
	private final StorageConfig storageConfig;
	private final BinlogInfo startBinlogInfo;
	private BinlogInfo lastWrittenBinlogInfo;
	private final ReentrantLock lock;
	private final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;

	private volatile WriteManager writeManager = null;

	public DefaultWriteChannel(StorageConfig storageConfig, BinlogInfo startBinlogInfo, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector, SourceType sourceType) {
		this.sourceType = sourceType;
		this.storageConfig = storageConfig;
		this.startBinlogInfo = startBinlogInfo;
		this.lastWrittenBinlogInfo = startBinlogInfo;
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
		this.readerTaskStatMetricsCollector.setStorageMode(storageConfig.getStorageMode());
		this.lock = new ReentrantLock();
	}

	@Override
	public void append(ChangeEntry dataValue) throws Exception {
		checkStop();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("append dbChangeEntry: {}", dataValue);
		}

		
		if (canSkipEvent(dataValue.getBinlogInfo())) {
			LOG.warn("Skip event binlogInfo: {}, lastWrittenBinlogInfo: {}", dataValue.getBinlogInfo(), this.lastWrittenBinlogInfo);
			return;
		}
		this.lock.lockInterruptibly();
		try {
			writeManager.append(dataValue);

			this.lastWrittenBinlogInfo = dataValue.getBinlogInfo();
			// monitor
			this.readerTaskStatMetricsCollector.incrEventWriteCounter();
			this.readerTaskStatMetricsCollector.setLastEventProducerToStorageCost(System.nanoTime() - dataValue.getReceiveNanos());
		} catch (Exception e){
			throw e;
		} finally {
			this.lock.unlock();
		}
	}

	
	private boolean canSkipEvent(BinlogInfo eventBinlogInfo) {
		switch (sourceType) {
			case MySQL:
				return lastWrittenBinlogInfo.isGreaterThan(eventBinlogInfo, storageConfig.getIndexPolicy());
			default:
				throw new PtubesRunTimeException("Unsupported binlog comparison in source type " + sourceType.name());
		}
	}

	@Override
	public void delete(String date) throws Exception {
		
	}

	@Override
	protected void doStart() {
		this.lock.lock();
		try {
			if (writeManager == null) {
				if (StorageConstant.StorageMode.MIX.equals(storageConfig.getStorageMode())) {
					writeManager = new MixWriteManager(storageConfig, startBinlogInfo, readerTaskStatMetricsCollector, sourceType);
				} else if (StorageConstant.StorageMode.FILE.equals(storageConfig.getStorageMode())) {
					writeManager = new FileWriteManager(storageConfig, startBinlogInfo, readerTaskStatMetricsCollector, sourceType);
				} else {
					writeManager = new MemWriteManager(storageConfig, startBinlogInfo, readerTaskStatMetricsCollector, sourceType);
				}
				LOG.info("Write Channel start");
				writeManager.start();
			}
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	protected void doStop() {
		this.lock.lock();
		try {
			if (writeManager != null) {
				LOG.info("Write Channel stop");
				writeManager.stop();
			}
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public Pair<BinlogInfo, BinlogInfo> getBinlogInfoRange() {
		return writeManager.getStorageRange();
	}

	
	@Override
	public StorageConstant.StorageRangeCheckResult isInStorage(BinlogInfo binlogInfo) {
		switch (sourceType) {
			case MySQL:
				return isInStorageForMySQL((MySQLBinlogInfo) binlogInfo);
			default:
				throw new PtubesRunTimeException("unsupported to compare binlog info in sourceType " + sourceType.name());
		}
	}

	private StorageConstant.StorageRangeCheckResult isInStorageForMySQL(MySQLBinlogInfo mySQLBinlogInfo) {
		Pair<BinlogInfo, BinlogInfo> binlogInfoRange = getBinlogInfoRange();
		if (!binlogInfoRange.getLeft().isGreaterThan(mySQLBinlogInfo, storageConfig.getIndexPolicy())) {
			// binlogInfo >= minBinlogInfo
			if (!mySQLBinlogInfo.isGreaterThan(binlogInfoRange.getRight(), storageConfig.getIndexPolicy())) {
				// binlogInfo <= maxBinlogInfo
				return StorageConstant.StorageRangeCheckResult.IN_RANGE;
			} else {
				// binlogInfo > maxBinlogInfo
				return StorageConstant.StorageRangeCheckResult.GREATER_THAN_MAX;
			}
		} else {
			// binlogInfo < minBinlogInfo
			return StorageConstant.StorageRangeCheckResult.LESS_THAN_MIN;
		}
	}

	public WriteManager getWriteManager() {
		this.lock.lock();
		try {
			return writeManager;
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public void switchStorageMode(StorageConstant.StorageMode storageMode) throws InterruptedException {
		StorageConstant.StorageMode oldStorageMode = writeManager.getStorageMode();
		this.lock.lockInterruptibly();
		try {
			if (oldStorageMode.equals(StorageConstant.StorageMode.MIX) && storageMode.equals(StorageConstant.StorageMode.MEM)) {
				storageConfig.setStorageMode(storageMode);
				MemWriteManager memWriteManager = ((MixWriteManager) this.writeManager).getMemWriteManager();
				FileWriteManager fileWriteManager = ((MixWriteManager) this.writeManager).getFileWriteManager();
				writeManager = memWriteManager;
				fileWriteManager.stop();
				readerTaskStatMetricsCollector.setFileStorageMinBinlogInfo(new MySQLBinlogInfo());
				readerTaskStatMetricsCollector.setFileStorageMaxBinlogInfo(new MySQLBinlogInfo());
			} else if (oldStorageMode.equals(StorageConstant.StorageMode.MEM) && storageMode.equals(StorageConstant.StorageMode.MIX)) {
				FileSystem.backupStorageDir(storageConfig.getReaderTaskName());
				storageConfig.setStorageMode(storageMode);
				writeManager = MixWriteManager.createAndStartMixWriteManager(storageConfig, ((MemWriteManager) this.writeManager), readerTaskStatMetricsCollector, sourceType);
			}
			this.readerTaskStatMetricsCollector.setStorageMode(storageConfig.getStorageMode());
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public void applyFileRetentionConfigChange(int fileRetentionHours, int fileCompressHours) throws InterruptedException {
		storageConfig.setFileRetentionHours(fileRetentionHours);
		storageConfig.setFileCompressHours(fileCompressHours);
		if (StorageConstant.StorageMode.MIX.equals(storageConfig.getStorageMode())) {
			this.lock.lockInterruptibly();
			try {
				FileWriteManager fileWriteManager = ((MixWriteManager) this.writeManager).getFileWriteManager();
				fileWriteManager.applyFileRetentionConfigChange(fileRetentionHours, fileCompressHours);
			} finally {
				this.lock.unlock();
			}
		}

	}

	@Override
	public StorageConstant.StorageMode getStorageMode() {
		return this.writeManager.getStorageMode();
	}
}
