package com.meituan.ptubes.reader.storage.mem;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import java.util.HashMap;
import java.util.Map;

public final class MemStorageFactory {
	private static final Logger LOG = LoggerFactory.getLogger(MemStorageFactory.class);
	private static volatile MemStorageFactory instance;

	protected MemStorageFactory() {
	}

	public static MemStorageFactory getInstance() {
		if (instance == null) {
			synchronized (MemStorageFactory.class) {
				if (instance == null) {
					instance = new MemStorageFactory();
				}
			}
		}
		return instance;
	}

	private final Map<String, PooledMemStorage> POOLED_MEM_STORAGE = new HashMap<>();

	public synchronized MemStorage getMemStorage(StorageConfig storageConfig, SourceType sourceType) {
		String physicalSourceName = storageConfig.getReaderTaskName();

		MemStorage memStorage;
		if (POOLED_MEM_STORAGE.containsKey(physicalSourceName)) {
			PooledMemStorage pooledMemStorage = POOLED_MEM_STORAGE.get(physicalSourceName);
			pooledMemStorage.retain();
			memStorage = pooledMemStorage.getMemStorage();
			LOG.info("Retain mem storage: {}, memStorage object: {}", physicalSourceName, memStorage.hashCode());
		} else {
			memStorage = initMemStorage(storageConfig, sourceType);
			POOLED_MEM_STORAGE.put(physicalSourceName, new PooledMemStorage(memStorage));
			LOG.info("Init mem storage: {}, memStorage object: {}", physicalSourceName, memStorage.hashCode());
		}

		return memStorage;
	}

	protected MemStorage initMemStorage(StorageConfig storageConfig, SourceType sourceType) {
		return new MemStorage(storageConfig, sourceType);
	}

	public synchronized void releaseMemStorage(StorageConfig storageConfig) {
		String physicalSourceName = storageConfig.getReaderTaskName();

		if (POOLED_MEM_STORAGE.containsKey(physicalSourceName)) {
			PooledMemStorage pooledMemStorage = POOLED_MEM_STORAGE.get(physicalSourceName);
			MemStorage memStorage = pooledMemStorage.getMemStorage();
			int refCnt = pooledMemStorage.release();
			LOG.info("Release mem storage: {}, memStorage object: {}", physicalSourceName, memStorage.hashCode());
			if (refCnt <= 0) {
				memStorage.closeBuffer(false);
				POOLED_MEM_STORAGE.remove(physicalSourceName);
				LOG.info("Recycle mem storage: {}, memStorage object: {}", physicalSourceName, memStorage.hashCode());
			}
		}
	}

	public synchronized void forceRecycleMemStorage(String readerTaskName) {
		if (POOLED_MEM_STORAGE.containsKey(readerTaskName)) {
			PooledMemStorage pooledMemStorage = POOLED_MEM_STORAGE.get(readerTaskName);
			MemStorage memStorage = pooledMemStorage.getMemStorage();
			memStorage.closeBuffer(false);
			POOLED_MEM_STORAGE.remove(readerTaskName);
			LOG.warn("Force Recycle mem storage: {}, memStorage object: {}", readerTaskName, memStorage.hashCode());
		}
	}

	public synchronized int getRefCount(String name) {
		if (POOLED_MEM_STORAGE.containsKey(name)) {
			return POOLED_MEM_STORAGE.get(name).getRefCnt().get();
		} else {
			return 0;
		}
	}
}
