package com.meituan.ptubes.reader.storage.file.index;

import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.storage.file.index.write.L1WriteIndexManager;
import com.meituan.ptubes.reader.storage.file.index.write.L2WriteIndexManager;
import java.io.File;
import com.meituan.ptubes.reader.storage.file.index.read.L1ReadIndexManager;
import com.meituan.ptubes.reader.storage.file.index.read.L2ReadIndexManager;

public final class IndexManagerFactory {

	private IndexManagerFactory() {
	}

	public static L1ReadIndexManager newL1ReadIndexManager(File file, StorageConfig storageConfig, SourceType sourceType) {
		return new L1ReadIndexManager(file, storageConfig.getFileConfig().getL1ReadBufSizeInByte(), storageConfig.getFileConfig().getL1ReadAvgSizeInByte(), storageConfig.getIndexPolicy(), sourceType);
	}

	public static L1WriteIndexManager newL1WriteIndexManager(File file, StorageConfig storageConfig, SourceType sourceType) {
		return new L1WriteIndexManager(file, storageConfig.getFileConfig().getL1WriteBufSizeInByte(), storageConfig.getFileConfig().getL1BucketSizeInByte(), storageConfig, sourceType);
	}

	public static L2ReadIndexManager newL2ReadIndexManager(File file, StorageConfig storageConfig, SourceType sourceType) {
		return new L2ReadIndexManager(file, storageConfig.getFileConfig().getL2ReadBufSizeInByte(), storageConfig.getFileConfig().getL2ReadAvgSizeInByte(), storageConfig.getIndexPolicy(), sourceType);
	}

	public static L2WriteIndexManager newL2WriteIndexManager(File file, String date, int number, StorageConfig.FileConfig fileConfig) {
		return new L2WriteIndexManager(file, date, number, fileConfig.getL2WriteBufSizeInByte(), fileConfig.getL2BucketSizeInByte());
	}

}
