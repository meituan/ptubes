package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.reader.container.common.config.ConfigService;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerBaseConfig;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;
import com.meituan.ptubes.reader.container.common.config.producer.RdsConfig;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;
import com.meituan.ptubes.reader.container.common.config.respository.MySQLConfigRespository;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import com.meituan.ptubes.reader.container.common.config.ConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLConfigRespositoryBuilder implements ConfigBuilder<MySQLConfigRespository> {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigBuilder.class);
    public static final MySQLConfigRespositoryBuilder INSTANCE = new MySQLConfigRespositoryBuilder();

    @Nonnull
    @Override public MySQLConfigRespository build(ConfigService configService, String readerTaskName) {
        ProducerBaseConfig producerBaseConfig = ProducerBaseConfigBuilder.BUILDER.build(configService, readerTaskName);
        Map<String, TableConfig> subscriptionConfig = SubscriptionConfigBuilder.BUILDER.build(configService, readerTaskName);
        RdsConfig rdsConfig = RdsConfigBuilder.BUILDER.build(configService, readerTaskName);

        ProducerConfig producerConfig;
        if (Objects.nonNull(producerBaseConfig) && Objects.nonNull(rdsConfig)/* && MapUtils.isNotEmpty(subscriptionConfig)*/) {
            producerConfig = new ProducerConfig(readerTaskName, producerBaseConfig, subscriptionConfig, rdsConfig);
        } else {
            throw new PtubesRunTimeException("load producer config for readerTask " + readerTaskName + " fail, please have a check");
        }

        StorageConfig storageConfig = StorageBaseConfigBuilder.BUILDER.build(configService, readerTaskName);
        StorageConfig.MemConfig memConfig = StorageMemConfigBuilder.BUILDER.build(configService, readerTaskName);
        StorageConfig.FileConfig fileConfig = StorageFileConfigBuilder.BUILDER.build(configService, readerTaskName);

        if (Objects.nonNull(storageConfig) && Objects.nonNull(memConfig) && Objects.nonNull(fileConfig)) {
            storageConfig.setReaderTaskName(readerTaskName);
            storageConfig.setMemConfig(memConfig);
            storageConfig.setFileConfig(fileConfig);
        } else {
            throw new PtubesRunTimeException("load storage config for readerTask " + readerTaskName + " fail, please have a check");
        }

        return new MySQLConfigRespository(producerConfig, storageConfig);
    }
}
