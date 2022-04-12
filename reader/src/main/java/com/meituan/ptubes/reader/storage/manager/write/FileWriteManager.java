package com.meituan.ptubes.reader.storage.manager.write;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntry;
import com.meituan.ptubes.reader.storage.common.event.ChangeEntryFactory;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.reader.storage.file.index.write.SeriesWriteIndexManager;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.data.write.GroupWriteDataManager;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;

public class FileWriteManager extends AbstractLifeCycle implements WriteManager<DataPosition, ChangeEntry> {
	private final Logger log;

	private final StorageConfig storageConfig;
	private final ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector;
	private final SourceType sourceType;

	private SeriesWriteIndexManager writeIndexManager;

	private GroupWriteDataManager writeDataManager;

	private BinlogInfo startBinlogInfo;

	private volatile BinlogInfo minBinlogInfo = null;

	private volatile BinlogInfo maxBinlogInfo = null;

	private DataPosition lastWrittenDataPosition = null;

	private Thread flushThread;

	private ScheduledExecutorService expireTask;


	public FileWriteManager(StorageConfig storageConfig, BinlogInfo startBinlogInfo, ReaderTaskStatMetricsCollector readerTaskStatMetricsCollector, SourceType sourceType) {
		this.storageConfig = storageConfig;
		this.startBinlogInfo = startBinlogInfo;
		this.readerTaskStatMetricsCollector = readerTaskStatMetricsCollector;
		this.sourceType = startBinlogInfo.getSourceType();
		log = LoggerFactory.getLogger(FileWriteManager.class + "_" + storageConfig.getReaderTaskName());
	}

	@Override
	protected void doStart() {
		if (writeIndexManager == null) {
			writeIndexManager = new SeriesWriteIndexManager(storageConfig, sourceType);
			writeIndexManager.start();
		}

		if (writeDataManager == null) {
			writeDataManager = new GroupWriteDataManager(
				storageConfig,
				startBinlogInfo
			);
			writeDataManager.start();
		}

		startFileStorage();

		fillBinlogInfoRange();

		// clean process/refresh process
		startScheduleTask();

	}

	private void startScheduleTask() {

		if (flushThread == null) {
			flushThread = new Thread(new FlushTask());
			flushThread.setName("flush-" + storageConfig.getReaderTaskName());
			flushThread.start();
		}

		if (expireTask == null) {
			Runnable runnable = getExpireRunnableTask();
			int maxDelayMin = storageConfig.getMaxExpireTaskDelayMinutes();
			if (maxDelayMin < 1) {
				maxDelayMin = 1;
			} else if (maxDelayMin > 60) {
				maxDelayMin = 60;
			}
			int delayMin = RandomUtils.nextInt(1, maxDelayMin);
			expireTask = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
				@Override public Thread newThread(Runnable r) {
					return new Thread(r, "expire-" + storageConfig.getReaderTaskName());
				}
			}, new ThreadPoolExecutor.DiscardOldestPolicy());
			expireTask.scheduleAtFixedRate(runnable,
										   delayMin,
										   60,
										   TimeUnit.MINUTES
			);
			log.info(
				"Expire task: {} will schedule {} minutes later",
				storageConfig.getReaderTaskName(),
				delayMin
			);
		}
	}

	private Runnable getExpireRunnableTask() {
		return new Runnable() {
			@Override
			public void run() {
				if (!isStopped() && !Thread.currentThread()
					.isInterrupted()) {
					try {
						log.info(
							"Expire task: {} scheduled, begin clean expired file",
							storageConfig.getReaderTaskName()
						);
						cleanExpireData();
						log.info(
							"Expire task: {}, finish clean expired file",
							storageConfig.getReaderTaskName()
						);
					} catch (Exception e) {
						log.error(
							"Expire task: {}, clean expired file failed!",
							storageConfig.getReaderTaskName(),
							e
						);
					}
					try {
						log.info(
							"Expire task: {} scheduled, begin compress file",
							storageConfig.getReaderTaskName()
						);
						FileSystem.compressFile(
							storageConfig.getReaderTaskName(),
							storageConfig.getFileCompressHours()
						);
						log.info(
							"Expire task: {}, finish compress file",
							storageConfig.getReaderTaskName()
						);
					} catch (Exception e) {
						log.error(
							"Expire task: {}, compress file failed!",
							storageConfig.getReaderTaskName(),
							e
						);
					}
				}
			}
		};
	}

	public void applyFileRetentionConfigChange(int fileRetentionHours, int fileCompressHours) {
		storageConfig.setFileRetentionHours(fileRetentionHours);
		storageConfig.setFileCompressHours(fileCompressHours);
	}

	private void startFileStorage() {
		// If there is no data, write a sentinel event at startup
		if (!FileSystem.l1IndexExist(storageConfig.getReaderTaskName())) {
			try {
				appendWithoutCheck(ChangeEntryFactory.createSentinelEntry(startBinlogInfo));
				flush();
			} catch (IOException e) {
				e.printStackTrace();
				throw new PtubesRunTimeException(e);
			}
		}
	}

	private void fillBinlogInfoRange() {
		try {
			minBinlogInfo = FileSystem.getMinBinlogInfo(storageConfig, sourceType);
		} catch (Exception e) {
			log.error("Get minBinlogInfo fail", e);
		}

		if (minBinlogInfo == null) {
			log.error("Get task: {} minBinlogInfo is null, set it to startBinlogInfo: {}", this.storageConfig.getReaderTaskName(), this.startBinlogInfo);
			minBinlogInfo = startBinlogInfo;
		}
		try {
			maxBinlogInfo = FileSystem.getMaxBinlogInfo(storageConfig, sourceType);
		} catch (IOException e) {
			log.error("Get task: {} maxBilogInfo fail", this.storageConfig.getReaderTaskName(), e);
		}
		if (maxBinlogInfo == null) {
			log.error("Get task: {} maxBinlogInfo is null, set it to startBinlogInfo: {}", this.storageConfig.getReaderTaskName(), this.startBinlogInfo);
			maxBinlogInfo = startBinlogInfo;
		}

		log.info("Task: {} MinBinlogInfo: {}", this.storageConfig.getReaderTaskName(), minBinlogInfo);
		log.info("Task: {} MaxBinlogInfo: {}", this.storageConfig.getReaderTaskName(), maxBinlogInfo);
		// monitor
		this.readerTaskStatMetricsCollector.setFileStorageMinBinlogInfo(minBinlogInfo);
		this.readerTaskStatMetricsCollector.setFileStorageMaxBinlogInfo(maxBinlogInfo);
	}

	@Override
	protected void doStop() {
		try {
			flush();
		} catch (IOException e) {
			log.warn("Task: {} Flush error: {}", this.storageConfig.getReaderTaskName(), e.getMessage());
		}

		if (writeIndexManager != null) {
			writeIndexManager.stop();
		}

		if (writeDataManager != null) {
			writeDataManager.stop();
		}

		if (flushThread != null) {
			flushThread.interrupt();
		}

		if (expireTask != null) {
			expireTask.shutdownNow();
		}
	}

	@Override
	public DataPosition append(ChangeEntry dataValue) throws IOException {
		checkStop();

		DataPosition dataPosition = appendWithoutCheck(dataValue);

		// monitor
		this.readerTaskStatMetricsCollector.setFileStorageMaxBinlogInfo(dataValue.getBinlogInfo());

		return dataPosition;
	}

	private DataPosition appendWithoutCheck(ChangeEntry dataValue) throws IOException {
		DataPosition dataPosition = writeDataManager.append(dataValue);

		// Sparse index, not all transaction writes will write the index file
		writeIndexManager.append(dataValue.getBinlogInfo(), dataPosition);

		maxBinlogInfo = dataValue.getBinlogInfo();

		// monitor
		this.readerTaskStatMetricsCollector.setFileStorageMaxBinlogInfo(maxBinlogInfo);

		return dataPosition;
	}

	@Override
	public void flush() throws IOException {
		if (isStopped()) {
			return;
		}

		writeIndexManager.flush();
		writeDataManager.flush();
	}

	@Override
	public DataPosition position() {
		return writeDataManager.position();
	}

	@Override
	public Pair<BinlogInfo, BinlogInfo> getStorageRange() {
		return Pair.of(minBinlogInfo, maxBinlogInfo);
	}

	private class FlushTask implements Runnable {
		@Override
		public void run() {
			while (!isStopped() && !Thread.currentThread().isInterrupted()) {
				try {
					flush();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						flush();
						Thread.currentThread().interrupt();
					}
				} catch (Exception e) {
					log.error("Flush failed!", e);
				}
			}
		}
	}

	public void cleanExpireData() {
		String readerTaskName = storageConfig.getReaderTaskName();

		// 1. Move the expired l2 index and data files to the expire folder 2. Delete the files that have been in the expire folder before
		FileSystem.retentionFile(readerTaskName, storageConfig.getFileRetentionHours());

		// Update minBinlogInfo (by traversing the l2 index under l1, find the key of the first non-empty l2 index, which is the smallest binlogInfo)
		BinlogInfo newMinBinlogInfo = FileSystem.getMinBinlogInfo(storageConfig, sourceType);

		if (newMinBinlogInfo == null) {
			log.error("Get minBinlogInfo is null, set it to startBinlogInfo: {}", this.startBinlogInfo);
			minBinlogInfo = startBinlogInfo;
		} else {
			minBinlogInfo = newMinBinlogInfo;
		}

		// monitor
		this.readerTaskStatMetricsCollector.setFileStorageMinBinlogInfo(minBinlogInfo);
	}

	@Override
	public StorageConstant.StorageMode getStorageMode() {
		return StorageConstant.StorageMode.FILE;
	}
}
