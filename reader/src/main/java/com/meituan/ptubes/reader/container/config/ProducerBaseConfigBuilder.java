package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerBaseConfig;
import javax.annotation.Nullable;
import com.meituan.ptubes.reader.container.common.config.ConfigBuilder;

public class ProducerBaseConfigBuilder implements ConfigBuilder<ProducerBaseConfig> {

    public static ProducerBaseConfigBuilder BUILDER = new ProducerBaseConfigBuilder();

    @Nullable
    @Override public ProducerBaseConfig build(ConfigService configService, String readerTaskName) {
        return new ProducerBaseConfig();
    }
}
