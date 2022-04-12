package com.meituan.ptubes.reader.storage.file.data;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.DateUtil;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.storage.file.data.read.SingleReadDataManager;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.data.write.SingleWriteDataManager;

public final class DataManagerFinder {
	private final static Logger LOG = LoggerFactory.getLogger(DataManagerFinder.class);
	/**
	 * fileName-referenceCount
	 */
	private final static Map<String, AtomicInteger> REF_COUNTER_MAP = new ConcurrentHashMap<>();

	public static SingleReadDataManager findReadDataManager(String physicalSourceName, StorageConfig.FileConfig fileConfig, DataPosition dataPosition)
			throws IOException {
		String date = String.valueOf(dataPosition.getCreationDate());
		int number = dataPosition.getBucketNumber();

		File file = FileSystem.visitDataFile(physicalSourceName, date, number);

		if (file == null) {
			return null;
		}

		openDataFile(file.getAbsolutePath());
		try {
			SingleReadDataManager result = DataManagerFactory.newSingleReadDataManager(file, fileConfig.getDataReadBufSizeInByte(), fileConfig.getDataReadAvgSizeInByte());
			result.start();
			result.open(dataPosition);
			return result;
		} catch (Exception e) {
			LOG.error("Start read data manager error, file: {}", file.getAbsolutePath(), e);
			releaseDataFile(file.getAbsolutePath());
			throw e;
		}
	}

	public static SingleReadDataManager findNextReadDataManager(String physicalSourceName, StorageConfig.FileConfig fileConfig, DataPosition dataPosition)
			throws IOException {
		String date = String.valueOf(dataPosition.getCreationDate());
		int number = dataPosition.getBucketNumber();

		File file = FileSystem.visitDataFile(physicalSourceName, date, ++number);
		if (file == null) {
			number = 0;
			// If it is not the v10 version, use the v1 version to parse to ensure that the old version of the event sent by the relay can be compatible when the client is upgraded to the v10 version
			int curTime = DateUtil.getDateHour(System.currentTimeMillis());
			date = DateUtil.getNextHour(date);
			while (Objects.nonNull(date) && Integer.valueOf(date) <= curTime) {
				file = FileSystem.visitDataFile(physicalSourceName, date, number);
				if(Objects.nonNull(file)){
					break;
				}
				date = DateUtil.getNextHour(date);
			}
		}

		if (file == null) {
			return null;
		}

		openDataFile(file.getAbsolutePath());
		try {
			SingleReadDataManager result = DataManagerFactory.newSingleReadDataManager(file, fileConfig.getDataReadBufSizeInByte(), fileConfig.getDataReadAvgSizeInByte());
			result.start();
			result.open(new DataPosition(date, number, 0));
			return result;
		} catch (Exception e) {
			LOG.error("Start read data manager error, file: {}", file.getAbsolutePath(), e);
			releaseDataFile(file.getAbsolutePath());
			throw e;
		}
	}

	public static SingleWriteDataManager findNextWriteDataManager(StorageConfig storageConfig, int date) throws IOException {
		String dateStr = String.valueOf(date);
		File file = FileSystem.nextDataFile(storageConfig.getReaderTaskName(), dateStr);
		int number = FileSystem.parseDataNumber(file);

		if (file == null) {
			return null;
		}

		openDataFile(file.getAbsolutePath());

		return DataManagerFactory.newSingleWriteDataManager(file, dateStr, number, storageConfig.getFileConfig());
	}

	private synchronized static void openDataFile(String fileName) {
		LOG.info("Open data file: {}", fileName);
		if (REF_COUNTER_MAP.containsKey(fileName)) {
			REF_COUNTER_MAP.get(fileName)
				.addAndGet(1);
		} else {
			REF_COUNTER_MAP.putIfAbsent(
				fileName,
				new AtomicInteger(1)
			);
		}
	}

	public synchronized static void releaseDataFile(String fileName) {
		LOG.info("Release data file: {}", fileName);
		int refCount = REF_COUNTER_MAP.getOrDefault(
			fileName,
			new AtomicInteger(1)
		)
			.decrementAndGet();
		if (refCount == 0) {
			REF_COUNTER_MAP.remove(fileName);
		}
	}

	public synchronized static int getRefCount(String fileName) {
		return REF_COUNTER_MAP.getOrDefault(
			fileName,
			new AtomicInteger(0)
		)
			.get();
	}
}
