package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import javax.annotation.Nullable;
import com.meituan.ptubes.reader.container.common.config.ConfigBuilder;

public class StorageMemConfigBuilder implements ConfigBuilder<StorageConfig.MemConfig> {

    public static StorageMemConfigBuilder BUILDER = new StorageMemConfigBuilder();

    @Nullable
    @Override public StorageConfig.MemConfig build(ConfigService configService, String readerTaskName) {
        long memBufferSize = Long.valueOf(configService.getConfig(ConfigKeyConstants.TaskConfiguration.genConfigKey(readerTaskName, ConfigKeyConstants.TaskConfiguration.STORAGE_MEM_BUFFERSIZE)));
        return new StorageConfig.MemConfig(memBufferSize);
    }
}
