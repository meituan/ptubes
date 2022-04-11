package com.meituan.ptubes.reader.storage.file.index;

import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.reader.storage.file.index.write.L1WriteIndexManager;
import com.meituan.ptubes.reader.storage.file.index.write.L2WriteIndexManager;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.meituan.ptubes.reader.storage.file.index.read.L1ReadIndexManager;
import com.meituan.ptubes.reader.storage.file.index.read.L2ReadIndexManager;

public final class SeriesIndexManagerFinder {
	private final static Map<String, AtomicInteger> REF_COUNTER_MAP = new ConcurrentHashMap<>();

	public static L1WriteIndexManager findL1WriteIndexManager(StorageConfig storageConfig, SourceType sourceType) throws IOException {
		File file = FileSystem.visitL1IndexFile(storageConfig.getReaderTaskName());
		if (file == null) {
			file = FileSystem.nextL1IndexFile(storageConfig.getReaderTaskName());
		}
		return IndexManagerFactory.newL1WriteIndexManager(file, storageConfig, sourceType);
	}

	public static L1ReadIndexManager findL1ReadIndexManager(StorageConfig storageConfig, SourceType sourceType) throws IOException {
		
		File file = FileSystem.visitL1IndexFile(storageConfig.getReaderTaskName());
		checkFileExists(file);
		return IndexManagerFactory.newL1ReadIndexManager(file, storageConfig, sourceType);
	}

	public static L2WriteIndexManager findNextL2WriteIndexManager(
		StorageConfig storageConfig,
		int date
	) throws IOException {
		String dateStr = String.valueOf(date);
		File file = FileSystem.nextL2IndexFile(
			storageConfig.getReaderTaskName(),
			dateStr
		);
		int number = FileSystem.parseL2IndexNumber(file);

		openIndexFile(file.getAbsolutePath());

		return IndexManagerFactory.newL2WriteIndexManager(
			file,
			dateStr,
			number,
			storageConfig.getFileConfig()
		);
	}

	public static L2ReadIndexManager findL2ReadIndexManager(StorageConfig storageConfig, DataPosition dataPosition,
		SourceType sourceType)
		throws IOException {
		
		String date = dataPosition.getCreationDate() + "";
		int number = dataPosition.getBucketNumber();
		File file = FileSystem.visitL2IndexFile(storageConfig.getReaderTaskName(), date, number);
		checkFileExists(file);

		openIndexFile(file.getAbsolutePath());

		return IndexManagerFactory.newL2ReadIndexManager(file, storageConfig, sourceType);
	}

	private synchronized static void openIndexFile(String fileName) {
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

	public synchronized static void releaseIndexFile(String fileName) {
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

	private static void checkFileExists(File file) throws FileNotFoundException {
		if (file == null) {
			throw new FileNotFoundException();
		} else if (!file.exists()) {
			throw new FileNotFoundException(file.getName());
		}
	}

	public static boolean isL2IndexFileExist(StorageConfig storageConfig, DataPosition dataPosition) {
		String date = dataPosition.getCreationDate() + "";
		int number = dataPosition.getBucketNumber();
		File file = FileSystem.visitL2IndexFile(storageConfig.getReaderTaskName(), date, number);
		try {
			checkFileExists(file);
		} catch (FileNotFoundException e) {
			return false;
		}
		return true;
	}

}
