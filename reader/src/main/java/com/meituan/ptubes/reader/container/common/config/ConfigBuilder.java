package com.meituan.ptubes.reader.container.common.config;

public interface ConfigBuilder<CONFIG_TYPE> {
    CONFIG_TYPE build(ConfigService configService, String readerTaskName);

}
