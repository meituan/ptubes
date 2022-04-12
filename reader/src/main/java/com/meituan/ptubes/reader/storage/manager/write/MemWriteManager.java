package com.meituan.ptubes.reader.storage.manager.write;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.storage.mem.MemStorage;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.mem.MemStorageFactory;
import org.apache.commons.lang3.tuple.Pair;


public class MemWriteManager extends AbstractLifeCycle implements WriteManager<DataPosition, ChangeEntry> {
	private static final Logger LOG = LoggerFactory.getLogger(FileWriteManager.class);

	private final SourceType sourceType;
	private final StorageConfig storageConfig;
	private final BinlogInfo startBinlogInfo;
	private final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;
	private MemStorage memStorage = null;

	public MemWriteManager(StorageConfig storageConfig, BinlogInfo startBinlogInfo, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector, SourceType sourceType) {
		this.sourceType = sourceType;
		this.storageConfig = storageConfig;
		this.startBinlogInfo = startBinlogInfo;
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
	}

	@Override
	public void doStart() {
		if (this.memStorage == null) {
			this.memStorage = MemStorageFactory.getInstance().getMemStorage(storageConfig, sourceType);
			this.memStorage.start(startBinlogInfo);
		}

		// monitor
		this.readerTaskStatMetricsCollector.setMemStorageMinBinlogInfo(this.memStorage.getMinBinlogInfo());
		this.readerTaskStatMetricsCollector.setMemStorageMaxBinlogInfo(this.memStorage.getLastWrittenBinlogInfo());
	}

	@Override
	public void doStop() {
		this.memStorage = null;
		MemStorageFactory.getInstance().releaseMemStorage(storageConfig);
	}

	@Override
	public DataPosition append(ChangeEntry dataValue) throws Exception {
		memStorage.append(dataValue);

		// monitor
		this.readerTaskStatMetricsCollector.setMemStorageMinBinlogInfo(this.memStorage.getMinBinlogInfo());
		this.readerTaskStatMetricsCollector.setMemStorageMaxBinlogInfo(this.memStorage.getLastWrittenBinlogInfo());

		return null;
	}

	@Override
	public void flush() throws IOException {
		// do nothing
	}

	@Override
	public DataPosition position() {
		return null;
	}

	public MemStorage getMemStorage() {
		return memStorage;
	}

	@Override
	public Pair<BinlogInfo, BinlogInfo> getStorageRange() {
		return Pair.of(memStorage.getMinBinlogInfo(), memStorage.getLastWrittenBinlogInfo());
	}

	@Override
	public StorageConstant.StorageMode getStorageMode() {
		return StorageConstant.StorageMode.MEM;
	}
}
