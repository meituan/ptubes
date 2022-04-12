package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import javax.annotation.Nullable;
import com.meituan.ptubes.reader.container.common.config.ConfigBuilder;

public class StorageFileConfigBuilder implements ConfigBuilder<StorageConfig.FileConfig> {

    public static StorageFileConfigBuilder BUILDER = new StorageFileConfigBuilder();

    @Nullable
    @Override public StorageConfig.FileConfig build(ConfigService configService, String readerTaskName) {
        return new StorageConfig.FileConfig();
    }
}
