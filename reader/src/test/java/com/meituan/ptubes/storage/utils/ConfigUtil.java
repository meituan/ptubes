package com.meituan.ptubes.storage.utils;


import com.meituan.ptubes.reader.container.common.constants.AssertLevel;
import com.meituan.ptubes.reader.container.common.constants.EventBufferConstants;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;

public class ConfigUtil {
	public static StorageConfig genStorageConfig(EventBufferConstants.AllocationPolicy allocationPolicy,
												 StorageConstant.IndexPolicy indexPolicy, StorageConstant.StorageMode storageMode, int maxSizeInByte, String name) {
		StorageConfig storageConfig = new StorageConfig();
		StorageConfig.MemConfig memConfig = genMemConfig(allocationPolicy, maxSizeInByte);
		StorageConfig.FileConfig fileConfig = genFileConfig();
		storageConfig.setFileConfig(fileConfig);
		storageConfig.setIndexPolicy(indexPolicy);
		storageConfig.setMemConfig(memConfig);
		storageConfig.setReaderTaskName(name);
		storageConfig.setStorageMode(storageMode);
		return storageConfig;
	}

	public static StorageConfig.MemConfig genMemConfig(EventBufferConstants.AllocationPolicy allocationPolicy,
			int maxSizeInByte) {
		StorageConfig.MemConfig memConfig = new StorageConfig.MemConfig();
		memConfig.setReadBufferSizeInByte(10 * StorageConstant.KB);
		memConfig.setAllocationPolicy(allocationPolicy);
		memConfig.setAssertLevel(AssertLevel.NONE);
		memConfig.setAverageEventSizeInByte(StorageConfig.DEFAULT_AVERAGE_EVENT_SIZE);
		memConfig.setBufferRemoveWaitPeriodSec(StorageConfig.BUFFER_REMOVE_WAIT_PERIOD);
		memConfig.setDefaultMemUsage(StorageConfig.DEFAULT_DEFAULT_MEMUSAGE);
		memConfig.setEnableIndex(true);
		memConfig.setMaxEventSizeInByte(StorageConfig.DEFAULT_MAX_EVENT_SIZE);
		memConfig.setMaxIndexSizeInByte((int) (100 * StorageConstant.KB));
		memConfig.setMaxIndividualBufferSizeInByte(maxSizeInByte);
		memConfig.setMaxSizeInByte(maxSizeInByte);
		memConfig.setRestoreMMappedBuffers(true);
		memConfig.setRestoreMMappedBuffersValidateEvents(true);
		return memConfig;
	}

	public static StorageConfig.FileConfig genFileConfig() {
		StorageConfig.FileConfig fileConfig = new StorageConfig.FileConfig(4 * StorageConstant.KB, 4 * StorageConstant.KB, 4 * StorageConstant.KB, 64 * StorageConstant.KB, 4 * StorageConstant.KB,
				1 * StorageConstant.KB, 1 * StorageConstant.KB, 1 * StorageConstant.KB, 16 * StorageConstant.KB, 1 * StorageConstant.KB, 1 * StorageConstant.KB, 1 * StorageConstant.KB, 4 * StorageConstant.KB);
		return fileConfig;
	}
}
