package com.meituan.ptubes.container.simulator.factory;


import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;

public class StorageConfigFactory {
    public static StorageConfig newStorageConfig() {
        return new StorageConfig();
    }
}
