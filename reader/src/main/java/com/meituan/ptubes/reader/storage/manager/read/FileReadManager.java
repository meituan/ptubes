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
import com.meituan.ptubes.reader.storage.common.event.ErrorEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.data.read.GroupReadDataManager;
import com.meituan.ptubes.reader.storage.file.index.read.SeriesReadIndexManager;
import org.apache.commons.lang3.tuple.Pair;

public class FileReadManager extends AbstractLifeCycle implements ReadManager<BinlogInfo, PtubesEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(FileReadManager.class);
	protected final StorageConfig storageConfig;
	private final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;
	private final SourceType sourceType;

	private GroupReadDataManager readDataManager;

	private SeriesReadIndexManager readIndexManager;

	private PtubesEvent firstEvent = null;

	private boolean isFirstRequest = false;

	public FileReadManager(StorageConfig storageConfig, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector,
		SourceType sourceType) {
		this.storageConfig = storageConfig;
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
		this.sourceType = sourceType;
	}

	@Override
	protected void doStart() {
		if (readIndexManager == null) {
			readIndexManager = new SeriesReadIndexManager(storageConfig, sourceType);
			readIndexManager.start();
		}

		if (readDataManager == null) {
			readDataManager = new GroupReadDataManager(storageConfig);
			readDataManager.start();
		}
	}

	@Override
	protected void doStop() {
		if (readIndexManager != null) {
			readIndexManager.stop();
			// bugfix: try switching to mem cause stopped lifecycle, need to clear readIndexManager so that it could start readIndexManager again while fileReadManager restart.
			readIndexManager = null;
		}
		LOG.info("ReadIndexManager stopped");
		if (readDataManager != null) {
			readDataManager.stop();
			readDataManager = null;
		}
		LOG.info("ReadDataManager stopped");
	}

	@Override
	public BinlogInfo openOldest() throws IOException {
		Pair<BinlogInfo, DataPosition> indexEntry = readIndexManager.findOldest();
		if (indexEntry == null) {
			throw new IOException("Failed to open oldest.");
		}
		readDataManager.open(new DataPosition(indexEntry.getValue()));
		return indexEntry.getKey();
	}

	@Override
	public BinlogInfo openLatest() throws IOException {
		Pair<BinlogInfo, DataPosition> indexEntry = readIndexManager.findLatest();
		if (indexEntry == null) {
			throw new IOException("Failed to open latest.");
		}
		readDataManager.open(new DataPosition(indexEntry.getValue()));
		BinlogInfo binlogInfo = indexEntry.getKey();
		PtubesEvent event = readDataManager.next();
		while (true) {
			if (EventType.isErrorEvent(event.getEventType())) {
				break;
			} else {
				binlogInfo = event.getBinlogInfo();
				event = readDataManager.next();
			}
		}
		return binlogInfo;
	}

	/**
	 *
	 * @param dataKey as searchBinlogInfo
	 * @return
	 * @throws IOException
	 * @throws LessThanStorageRangeException
	 * @throws GreaterThanStorageRangeException
	 */
	@Override
	public BinlogInfo open(BinlogInfo dataKey)
			throws IOException, LessThanStorageRangeException, GreaterThanStorageRangeException {
		Pair<BinlogInfo, DataPosition> indexEntry = readIndexManager.find(dataKey);
		if (indexEntry == null) {
			throw new IOException("Failed to open, binlogInfo = " + dataKey);
		}
		readDataManager.open(new DataPosition(indexEntry.getValue()));

		BinlogInfo binlogInfo = dataKey;
		PtubesEvent event = readDataManager.next();
		while (true) {
			if (!EventType.isErrorEvent(event.getEventType()) && isFirstEvent(event.getBinlogInfo(), dataKey)/*event.getBinlogInfo().isGreaterThan(dataKey, storageConfig.getIndexPolicy())*/) {
				firstEvent = event;
				isFirstRequest = true;
				binlogInfo = event.getBinlogInfo();
				break;
			} else {
				if (EventType.NO_MORE_EVENT.equals(event.getEventType())) {
					throw new GreaterThanStorageRangeException("BinlogInfo is greater than storage range, binlogInfo: " + dataKey);
				} else {
					event = readDataManager.next();
				}
			}
		}
		return binlogInfo;
	}
	private boolean isFirstEvent(BinlogInfo eventBinlogInfo, BinlogInfo searchBinlogInfo) {
		switch (sourceType) {
			case MySQL:
				return eventBinlogInfo.isGreaterThan(searchBinlogInfo, storageConfig.getIndexPolicy());
			default:
				throw new PtubesRunTimeException("Unsupported binlog info comparison in source type " + sourceType.name());
		}
	}

	@Override
	public PtubesEvent next() {
		// monitor
		this.readerTaskStatMetricsCollector.incrFileStorageEventReadCounter();
		try {
			if (isFirstRequest) {
				isFirstRequest = false;
				return firstEvent;
			}
			return readDataManager.next();
		} catch (IOException e) {
			LOG.info("FileReaderManager read next IOException");
			return ErrorEvent.NO_MORE_EVENT;
		}
	}

	@Override
	public MySQLBinlogInfo position() {
		
		return null;
	}

	@Override
	public StorageConstant.StorageMode getStorageMode() {
		return StorageConstant.StorageMode.FILE;
	}

	@Override
	public StorageConstant.StorageMode getCurrentStorageMode() {
		return getStorageMode();
	}
}
