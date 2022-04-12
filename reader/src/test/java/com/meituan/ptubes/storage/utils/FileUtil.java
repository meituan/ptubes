package com.meituan.ptubes.storage.utils;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.storage.file.filesystem.FileSystem;
import com.meituan.ptubes.reader.storage.utils.CodecUtil;
import java.io.File;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.storage.common.DataPosition;
import com.meituan.ptubes.reader.storage.file.index.read.L2ReadIndexManager;
import org.apache.commons.lang3.tuple.Pair;


public class FileUtil {

	public static void createFile(File file) throws IOException {
		File parent = file.getParentFile();
		if (!parent.exists() && !parent.mkdirs()) {
			throw new IOException("failed to create file parent directories.");
		}
		file.createNewFile();
	}

	public static void delFile(File file) {
		if (!file.exists()) {
			return;
		}

		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (File f : files) {
				delFile(f);
			}
		}
		file.delete();
	}

	public static Pair<BinlogInfo, DataPosition> getMaxIndexEntry(File file, StorageConfig storageConfig) throws IOException {
		L2ReadIndexManager l2ReadIndexManager = new L2ReadIndexManager(file, storageConfig.getFileConfig().getL2ReadBufSizeInByte(), storageConfig.getFileConfig().getL2ReadAvgSizeInByte(), storageConfig.getIndexPolicy(), SourceType.MySQL);
		l2ReadIndexManager.start();
		Pair<BinlogInfo, DataPosition> result = l2ReadIndexManager.findLatest();
		l2ReadIndexManager.stop();
		return result;
	}

	public static Pair<BinlogInfo, DataPosition> findIndexEntry(File file, StorageConfig storageConfig, long count) throws IOException {
		L2ReadIndexManager l2ReadIndexManager = new L2ReadIndexManager(file, storageConfig.getFileConfig().getL2ReadBufSizeInByte(), storageConfig.getFileConfig().getL2ReadAvgSizeInByte(), storageConfig.getIndexPolicy(), SourceType.MySQL);
		l2ReadIndexManager.start();
		l2ReadIndexManager.getReadBucket().skip(count * (MySQLBinlogInfo.getSizeInByte() + DataPosition.getSizeInByte() + 4));
		Pair<BinlogInfo, DataPosition> result = CodecUtil.decodeL2Index(l2ReadIndexManager.getReadBucket().next(), SourceType.MySQL);
		l2ReadIndexManager.stop();
		return result;
	}

	// Find the largest index not greater than position entry
	public static Pair<BinlogInfo, DataPosition> findLatestIndexEntry(StorageConfig storageConfig, DataPosition position) throws IOException {
		int maxBucketNumber = FileSystem.maxL2IndexFileNumber(storageConfig.getReaderTaskName(), position.getCreationDate() + "");
		File file = null;
		for (int i = maxBucketNumber; i >= 0; i--) {
			File l2IndexFile = FileSystem.visitL2IndexFile(storageConfig.getReaderTaskName(), position.getCreationDate() + "", i);
			Pair<BinlogInfo, DataPosition> entry = findIndexEntry(l2IndexFile, storageConfig, 0);
			if (isGreater(position, entry.getValue())) {
				file = l2IndexFile;
				break;
			}
		}
		L2ReadIndexManager l2ReadIndexManager = new L2ReadIndexManager(file, storageConfig.getFileConfig().getL2ReadBufSizeInByte(), storageConfig.getFileConfig().getL2ReadAvgSizeInByte(), storageConfig.getIndexPolicy(), SourceType.MySQL);
		l2ReadIndexManager.start();

		Pair<BinlogInfo, DataPosition> prevEntry = l2ReadIndexManager.next();
		while(true) {
			Pair<BinlogInfo, DataPosition> entry = l2ReadIndexManager.next();
			if (isGreater(entry.getValue(), position)) {
				return prevEntry;
			}
			prevEntry = entry;
		}
	}

	private static boolean isGreater(DataPosition position1, DataPosition position2) {
		if (position1.getCreationDate() != position2.getCreationDate()) {
			return position1.getCreationDate() > position2.getCreationDate();
		} else if (position1.getBucketNumber() != position2.getBucketNumber()) {
			return position1.getBucketNumber() > position2.getBucketNumber();
		} else {
			return position1.getOffset() > position2.getOffset();
		}
	}
}
