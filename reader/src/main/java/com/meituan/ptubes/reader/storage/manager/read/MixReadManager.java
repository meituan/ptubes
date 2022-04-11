package com.meituan.ptubes.reader.storage.manager.read;

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

public class MixReadManager extends AbstractLifeCycle implements ReadManager<BinlogInfo, PtubesEvent> {
	private final Logger log;

	protected final StorageConfig storageConfig;
	private final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;
	private final SourceType sourceType;

	private FileReadManager fileReadManager = null;

	private MemReadManager memReadManager = null;

	private BinlogInfo lastBinlogInfo = null;

	private boolean currentIsMemory = false;

	public MixReadManager(StorageConfig storageConfig, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector,
		SourceType sourceType) {
		this.storageConfig = storageConfig;
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
		this.sourceType = sourceType;
		log = LoggerFactory.getLogger(FileReadManager.class + "_" + storageConfig.getReaderTaskName());
	}

	public MixReadManager(StorageConfig storageConfig, FileReadManager fileReadManager, MemReadManager memReadManager,
			boolean currentIsMemory, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector, SourceType sourceType) {
		this.storageConfig = storageConfig;
		this.fileReadManager = fileReadManager;
		this.memReadManager = memReadManager;
		this.currentIsMemory = currentIsMemory;
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
		this.sourceType = sourceType;
		log = LoggerFactory.getLogger(FileReadManager.class + "_" + storageConfig.getReaderTaskName());
	}

	public static MixReadManager createAndStartMixReadManager(StorageConfig storageConfig,
			MemReadManager memReadManager, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector, SourceType sourceType) {
		FileReadManager fileReadManager = new FileReadManager(storageConfig, readerTaskStatMetricsCollector, sourceType);
		fileReadManager.start();
		MixReadManager mixReadManager = new MixReadManager(storageConfig, fileReadManager, memReadManager, true,
				readerTaskStatMetricsCollector, sourceType);
		mixReadManager.start();
		return mixReadManager;
	}

	@Override
	protected void doStart() {
		if (fileReadManager == null) {
			fileReadManager = new FileReadManager(storageConfig, readerTaskStatMetricsCollector, sourceType);
			fileReadManager.start();
		}

		if (memReadManager == null) {
			memReadManager = new MemReadManager(storageConfig, readerTaskStatMetricsCollector, sourceType);
			memReadManager.start();
		}
	}

	@Override
	protected void doStop() {
		if (fileReadManager != null) {
			fileReadManager.stop();
		}
		log.info("FileReadManager stopped");
		if (memReadManager != null) {
			memReadManager.stop();
		}
		log.info("MemReadManager stopped");
	}

	/**
	 * case 1:
	 * 			switch time
	 * 		        Y
	 * mem  |--------------------|
	 * file         |------------|
	 *
	 * case 2: (A few minutes later)
	 *   switch time
	 *      Y
	 * mem          |--------------------|
	 * file |----------------------------|
	 * @return
	 * @throws IOException
	 */
	@Override
	public BinlogInfo openOldest() throws IOException {
		return fileReadManager.openOldest();
	}

	@Override
	public BinlogInfo openLatest() throws IOException {
		BinlogInfo binlogInfo = memReadManager.openLatest();
		currentIsMemory = true;
		return binlogInfo;
	}

	@Override
	public BinlogInfo open(BinlogInfo binlogInfo)
			throws IOException, LessThanStorageRangeException, GreaterThanStorageRangeException {
		if (!StorageConstant.StorageRangeCheckResult.LESS_THAN_MIN.equals(
				memReadManager.containBinlogInfo(binlogInfo))) { // Is there any possibility of losing data here? It is still within the scope when it is checked, but the data is flushed when it is pulled?
			memReadManager.open(binlogInfo);
			currentIsMemory = true;
		} else {
			fileReadManager.open(binlogInfo);
		}
		return binlogInfo; // Confirm if there is a problem. The incoming binlogInfo returned here is not the BinlogInfo obtained from the file
	}

	@Override
	public PtubesEvent next() throws IOException {
		if (currentIsMemory) {
			try {
				return memoryNext();
			} catch (Throwable te) {
				try {
					switchToFile();
				} catch (LessThanStorageRangeException e1) {
					return ErrorEvent.NOT_IN_BUFFER;
				} catch (GreaterThanStorageRangeException e1) {
					return ErrorEvent.NO_MORE_EVENT;
				}
				return fileNext();
			}
		} else {
			PtubesEvent event = fileNext();
			trySwitchToMemory();
			return event;
		}
	}

	protected PtubesEvent fileNext() {
		PtubesEvent event = fileReadManager.next();

		if (event != null) {
			lastBinlogInfo = event.getBinlogInfo();
		}
		return event;
	}

	protected PtubesEvent memoryNext() throws IOException {
		PtubesEvent event = memReadManager.next();
		if (EventType.isErrorEvent(event.getEventType()) && !event.equals(ErrorEvent.NO_MORE_EVENT)) {
			throw new IOException("Get event from mem error");
		}
		if (event.getBinlogInfo() != null) {
			lastBinlogInfo = event.getBinlogInfo();
		}
		return event;
	}

	protected void trySwitchToMemory() {
		if (lastBinlogInfo != null && !StorageConstant.StorageRangeCheckResult.LESS_THAN_MIN.equals(
				memReadManager.containBinlogInfo(lastBinlogInfo))) {
			try {
				memReadManager.open(lastBinlogInfo);
			} catch (Exception e) {
				log.error("{} switch to mem fail", storageConfig.getReaderTaskName(), e);
				return;
			}
			// Do not use the reference count that needs to be released by stop
			fileReadManager.stop();
			currentIsMemory = true;
			log.info("{} switch to mem", storageConfig.getReaderTaskName());
		}
	}

	protected void switchToFile() throws IOException, LessThanStorageRangeException, GreaterThanStorageRangeException {
		if (lastBinlogInfo == null) {
			throw new IOException("lastBinlogInfo is null, can not switch to file");
		}
		fileReadManager.start();
		fileReadManager.open(lastBinlogInfo);
		currentIsMemory = false;
		log.info("{} switch to file", storageConfig.getReaderTaskName());
	}

	@Override
	public MySQLBinlogInfo position() {
		return null;
	}

	@Override
	public StorageConstant.StorageMode getStorageMode() {
		return StorageConstant.StorageMode.MIX;
	}

	@Override
	public StorageConstant.StorageMode getCurrentStorageMode() {
		return currentIsMemory ? StorageConstant.StorageMode.MEM : StorageConstant.StorageMode.FILE;
	}

	public FileReadManager getFileReadManager() {
		return fileReadManager;
	}

	public MemReadManager getMemReadManager() {
		return memReadManager;
	}
}
