package com.meituan.ptubes.reader.storage.manager;

import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.storage.manager.read.MixReadManager;
import com.meituan.ptubes.reader.storage.manager.write.MemWriteManager;
import com.meituan.ptubes.reader.storage.manager.write.MixWriteManager;

public final class ManagerFactory {
	public static MixReadManager newMixReadManager() {
		return null;
	}

	public static MixWriteManager newMixWriteManager(StorageConfig storageConfig) {
		return null;
	}

	public static MemWriteManager newMemWriteManager(StorageConfig storageConfig) {
		return null;
	}

}
