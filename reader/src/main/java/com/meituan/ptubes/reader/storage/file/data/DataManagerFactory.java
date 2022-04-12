package com.meituan.ptubes.reader.storage.file.data;

import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.storage.file.data.read.SingleReadDataManager;
import java.io.File;
import com.meituan.ptubes.reader.storage.file.data.write.SingleWriteDataManager;

public final class DataManagerFactory {

	private DataManagerFactory() {
	}

	public static SingleReadDataManager newSingleReadDataManager(File file, int readBufSizeByte, int readAvgSizeByte) {
		return new SingleReadDataManager(file, readBufSizeByte, readAvgSizeByte);
	}

	public static SingleWriteDataManager newSingleWriteDataManager(File file, String date, int number, StorageConfig.FileConfig fileConfig) {
		return new SingleWriteDataManager(file, date, number, fileConfig);
	}
}
