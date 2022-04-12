package com.meituan.ptubes.reader.storage.manager.write;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import org.apache.commons.lang3.tuple.Pair;


public class MixWriteManager extends AbstractLifeCycle implements WriteManager<DataPosition, ChangeEntry> {
	private static final Logger LOG = LoggerFactory.getLogger(MixWriteManager.class);

	private final SourceType sourceType;
	private final StorageConfig storageConfig;
	private final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;

	private FileWriteManager fileWriteManager;

	private MemWriteManager memWriteManager;

	public MixWriteManager(StorageConfig storageConfig, BinlogInfo startBinlogInfo, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector, SourceType sourceType) {
		this.sourceType = sourceType;
		this.storageConfig = storageConfig;
		this.fileWriteManager = new FileWriteManager(storageConfig, startBinlogInfo, readerTaskStatMetricsCollector, sourceType);
		this.memWriteManager = new MemWriteManager(storageConfig, startBinlogInfo, readerTaskStatMetricsCollector, sourceType);
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
	}

	public MixWriteManager(StorageConfig storageConfig, FileWriteManager fileWriteManager,
			MemWriteManager memWriteManager, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector, SourceType sourceType) {
		this.sourceType = sourceType;
		this.storageConfig = storageConfig;
		this.fileWriteManager = fileWriteManager;
		this.memWriteManager = memWriteManager;
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
	}

	public static MixWriteManager createAndStartMixWriteManager(StorageConfig storageConfig, MemWriteManager memWriteManager, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector, SourceType sourceType) {
		FileWriteManager fileWriteManager = new FileWriteManager(storageConfig, memWriteManager.getMemStorage().getLastWrittenBinlogInfo(), readerTaskStatMetricsCollector, sourceType);
		fileWriteManager.start();
		MixWriteManager mixWriteManager = new MixWriteManager(storageConfig, fileWriteManager, memWriteManager, readerTaskStatMetricsCollector, sourceType);
		mixWriteManager.start();
		return mixWriteManager;
	}

	@Override
	protected void doStart() {
		if (fileWriteManager.isStopped()) {
			fileWriteManager.start();
		}
		if (memWriteManager.isStopped()) {
			memWriteManager.start();
		}
	}

	@Override
	protected void doStop() {
		if (memWriteManager != null) {
			memWriteManager.stop();
		}
		if (fileWriteManager != null) {
			fileWriteManager.stop();
		}
	}

	@Override
	public DataPosition append(ChangeEntry dataValue) throws Exception {
		DataPosition dataPosition = fileWriteManager.append(dataValue);
		memWriteManager.append(dataValue);
		return dataPosition;
	}

	@Override
	public void flush() throws IOException {
		fileWriteManager.flush();
	}

	@Override
	public DataPosition position() {
		return fileWriteManager.position();
	}

	@Override
	public Pair<BinlogInfo, BinlogInfo> getStorageRange() {
		Pair<BinlogInfo, BinlogInfo> memStorageRange = memWriteManager.getStorageRange();
		Pair<BinlogInfo, BinlogInfo> fileStorageRange = fileWriteManager.getStorageRange();
		/*memStorageRange.getLeft().isGreaterThan(fileStorageRange.getLeft(), storageConfig.getIndexPolicy())*/
		if (validateFileStorageRange(memStorageRange.getLeft(), fileStorageRange.getLeft())) {
			return fileStorageRange;
		} else {
			return memStorageRange;
		}
	}
	private boolean validateFileStorageRange(BinlogInfo minMemBinlogInfo, BinlogInfo minFileBinlogInfo) {
		switch (sourceType) {
			case MySQL:
				return minMemBinlogInfo.isGreaterThan(minFileBinlogInfo, storageConfig.getIndexPolicy());
			default:
				throw new PtubesRunTimeException("Unsupported binlog info comparison in source type " + sourceType.name());
		}
	}

	public MemWriteManager getMemWriteManager() {
		return memWriteManager;
	}

	public FileWriteManager getFileWriteManager() {
		return fileWriteManager;
	}

	@Override
	public StorageConstant.StorageMode getStorageMode() {
		return StorageConstant.StorageMode.MIX;
	}
}
