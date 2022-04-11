package com.meituan.ptubes.reader.storage.channel;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.handler.ChangeIdInfoReaderWriter;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import com.meituan.ptubes.reader.storage.filter.EventFilter;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.manager.read.FileReadManager;
import com.meituan.ptubes.reader.storage.manager.read.MemReadManager;
import com.meituan.ptubes.reader.storage.manager.read.MixReadManager;
import com.meituan.ptubes.reader.storage.manager.read.ReadManager;
import com.meituan.ptubes.reader.storage.mem.buffer.BinlogInfoFactory;


public class DefaultReadChannel extends AbstractLifeCycle implements ReadChannel {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultWriteChannel.class);
	private final StorageConfig storageConfig;
	private final EventFilter eventFilter;
	private final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;
	private final SourceType sourceType;

	protected final ReentrantLock lock;
	protected volatile ReadManager readManager;
	protected BinlogInfo lastBinlogInfo;

	public DefaultReadChannel(StorageConfig storageConfig, EventFilter eventFilter,
			ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector, SourceType sourceType) {
		this.storageConfig = storageConfig;
		this.eventFilter = eventFilter;
		this.lock = new ReentrantLock();
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
		this.sourceType = sourceType;
	}

	@Override
	protected void doStart() {
		this.lock.lock();
		try {
			if (StorageConstant.StorageMode.MIX.equals(storageConfig.getStorageMode())) {
				readManager = new MixReadManager(storageConfig, readerTaskStatMetricsCollector, sourceType);
			} else if (StorageConstant.StorageMode.FILE.equals(storageConfig.getStorageMode())) {
				readManager = new FileReadManager(storageConfig, readerTaskStatMetricsCollector, sourceType);
			} else {
				readManager = new MemReadManager(storageConfig, readerTaskStatMetricsCollector, sourceType);
			}
			readManager.start();
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	protected void doStop() {
		this.lock.lock();
		try {
			LOG.info("Begin to stop readManager");
			readManager.stop();
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public void openOldest() throws PtubesException, IOException {
		this.lock.lock();
		try {
			this.lastBinlogInfo = (BinlogInfo) readManager.openOldest();
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public void openLatest() throws PtubesException, IOException {
		this.lock.lock();
		try {
			this.lastBinlogInfo = (BinlogInfo) readManager.openLatest();
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public void open(BinlogInfo binlogInfo)
			throws LessThanStorageRangeException, GreaterThanStorageRangeException, IOException, PtubesException {
		this.lock.lock();
		try {
			if (SourceType.MySQL.equals(binlogInfo.getSourceType())) {
				assert binlogInfo instanceof MySQLBinlogInfo;
				if (((MySQLBinlogInfo) binlogInfo).getChangeId() < 0) {
					ChangeIdInfoReaderWriter.fillChangeId(storageConfig.getReaderTaskName(), (MySQLBinlogInfo) binlogInfo);
				}
			}

			this.lastBinlogInfo = binlogInfo;
			BinlogInfo searchBinlogInfo = BinlogInfoFactory.genSearchBinlogInfo(binlogInfo.getSourceType(), binlogInfo);
			readManager.open(searchBinlogInfo);
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public PtubesEvent next() throws IOException {
		long startReadTimeNS = System.nanoTime();
		this.lock.lock();
		try {
			PtubesEvent event = (PtubesEvent) readManager.next();
			while (!eventFilter.allow(event)) {
				event = (PtubesEvent) readManager.next();
			}

			long nextEventDuration = System.nanoTime() - startReadTimeNS;
			if (!EventType.isErrorEvent(event.getEventType())) {
				this.lastBinlogInfo = event.getBinlogInfo();
			}

			// report to monitor
			this.readerTaskStatMetricsCollector.incrEventReadCounter();
			this.readerTaskStatMetricsCollector.setLastEventStorageToReadChannelCost(nextEventDuration);

			return event;
		} catch (Exception e) {
			throw e;
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public StorageConstant.StorageStatus getStatus() {
		
		return null;
	}

	@Override
	public StorageConstant.StorageMode getStorageMode() {
		return readManager.getStorageMode();
	}

	@Override
	public String getBinlogInfoRange() {
		
		return null;
	}

	public ReadManager getReadManager() {
		return readManager;
	}

	@Override
	public void switchStorageMode(StorageConstant.StorageMode storageMode)
			throws InterruptedException, LessThanStorageRangeException {
		StorageConstant.StorageMode oldStorageMode = readManager.getStorageMode();
		this.lock.lockInterruptibly();
		try {
			if (oldStorageMode.equals(StorageConstant.StorageMode.MIX) && storageMode.equals(
					StorageConstant.StorageMode.MEM)) {
				storageConfig.setStorageMode(storageMode);
				MemReadManager memReadManager = ((MixReadManager) this.readManager).getMemReadManager();
				if (StorageConstant.StorageRangeCheckResult.LESS_THAN_MIN == memReadManager.containBinlogInfo(
						this.lastBinlogInfo)) {
					throw new LessThanStorageRangeException("LastBinlogInfo: " + this.lastBinlogInfo
							+ " is less than mem storage min binlogInfo, can not switch storage mode from MIX to MEM");
				}
				if (this.readManager.getCurrentStorageMode() != StorageConstant.StorageMode.MEM) {
					throw new LessThanStorageRangeException(
							"MixStorage currentIsMemeory is false, can not swith to mem" + this.lastBinlogInfo);
				}
				FileReadManager fileReadManager = ((MixReadManager) this.readManager).getFileReadManager();
				readManager = memReadManager;
				fileReadManager.stop();
			} else if (oldStorageMode.equals(StorageConstant.StorageMode.MEM) && storageMode.equals(
					StorageConstant.StorageMode.MIX)) {
				storageConfig.setStorageMode(storageMode);
				readManager = MixReadManager.createAndStartMixReadManager(storageConfig,
						((MemReadManager) this.readManager), readerTaskStatMetricsCollector, sourceType);
			}
		} finally {
			this.lock.unlock();
		}
	}
}
