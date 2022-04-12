package com.meituan.ptubes.reader.storage.manager.read;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import com.meituan.ptubes.reader.storage.mem.MemStorage;
import com.meituan.ptubes.reader.storage.mem.buffer.PtubesEventBuffer;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.mem.MemStorageFactory;

public class MemReadManager extends AbstractLifeCycle implements ReadManager<BinlogInfo, PtubesEvent> {
	private final Logger log;
	protected final StorageConfig storageConfig;
	private final String iteratorName;
	private final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;
	private final SourceType sourceType;
	protected MemStorage memStorage = null;
	protected PtubesEventBuffer.PtubesEventIterator iterator;

	private BinlogInfo prevBinlogInfo = null;

	public MemReadManager(StorageConfig storageConfig, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector,
		SourceType sourceType) {
		this.storageConfig = storageConfig;
		this.iteratorName = storageConfig.getReaderTaskName() + ".streamEventsIterator";
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
		this.sourceType = sourceType;
		log = LoggerFactory.getLogger(FileReadManager.class + "_" + storageConfig.getReaderTaskName());
	}

	@Override
	protected void doStart() {
		if (memStorage == null) {
			memStorage = MemStorageFactory.getInstance().getMemStorage(storageConfig, sourceType);
		}
	}

	@Override
	protected void doStop() {
		if (iterator != null) {
			memStorage.releaseIterator(iterator);
		}
		memStorage = null;
		MemStorageFactory.getInstance().releaseMemStorage(storageConfig);
		log.info("MemStorage stopped");
	}

	@Override
	public BinlogInfo open(BinlogInfo binlogInfo)
			throws IOException, LessThanStorageRangeException, GreaterThanStorageRangeException {
		prevBinlogInfo = binlogInfo;
		iterator = memStorage.getNormalInternalIterator(binlogInfo, iteratorName);
		return binlogInfo;
	}

	@Override
	public BinlogInfo openOldest() throws IOException {
		iterator = memStorage.getOldestInternalIterator(iteratorName);
		return memStorage.getMinBinlogInfo();
	}

	@Override
	public BinlogInfo openLatest() throws IOException {
		try {
			iterator = memStorage.getLatestInternalIterator(iteratorName);
		} catch (GreaterThanStorageRangeException e) {
			throw new IOException(e);
		} catch (LessThanStorageRangeException e) {
			throw new IOException(e);
		}
		return memStorage.getLastWrittenBinlogInfo();
	}

	@Override
	public PtubesEvent next() throws IOException {
		// monitor
		this.readerTaskStatMetricsCollector.incrMemStorageEventReadCounter();

		PtubesEvent event = iterator.next();
		if (!EventType.isErrorEvent(event.getEventType())) {
			prevBinlogInfo = event.getBinlogInfo();
		}
		return event;
	}

	@Override
	public MySQLBinlogInfo position() {
		return null;
	}

	@Override
	public StorageConstant.StorageMode getStorageMode() {
		return StorageConstant.StorageMode.MEM;
	}

	@Override
	public StorageConstant.StorageMode getCurrentStorageMode() {
		return getStorageMode();
	}

	
	public StorageConstant.StorageRangeCheckResult containBinlogInfo(BinlogInfo binlogInfo) {
		switch (sourceType) {
			case MySQL:
				return containMySQLBinlogInfo(binlogInfo);
			default:
				throw new PtubesRunTimeException("Unsupported binlog info comparison in source type " + sourceType.name());
		}
	}
	private StorageConstant.StorageRangeCheckResult containMySQLBinlogInfo(BinlogInfo binlogInfo) {
		if (binlogInfo.isGreaterEqualThan(memStorage.getMinBinlogInfo(), storageConfig.getIndexPolicy())) {
			if (binlogInfo.isGreaterEqualThan(memStorage.getLastWrittenBinlogInfo(), storageConfig.getIndexPolicy())) {
				return StorageConstant.StorageRangeCheckResult.GREATER_THAN_MAX;
			} else {
				return StorageConstant.StorageRangeCheckResult.IN_RANGE;
			}
		} else {
			return StorageConstant.StorageRangeCheckResult.LESS_THAN_MIN;
		}
	}

	public MemStorage getMemStorage() {
		return memStorage;
	}
}
