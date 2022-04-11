package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import javax.annotation.Nullable;
import com.meituan.ptubes.reader.container.common.config.ConfigBuilder;

public class StorageBaseConfigBuilder implements ConfigBuilder<StorageConfig> {

    public static StorageBaseConfigBuilder BUILDER = new StorageBaseConfigBuilder();

    @Nullable
    @Override public StorageConfig build(ConfigService configService, String readerTaskName) {
        StorageConstant.StorageMode storageMode = StorageConstant.StorageMode.valueOf(configService.getConfig(ConfigKeyConstants.TaskConfiguration.genConfigKey(readerTaskName, ConfigKeyConstants.TaskConfiguration.STORAGE_MODE)));
        StorageConfig basicStorageConfig = new StorageConfig(storageMode);
        return basicStorageConfig;
    }
}
