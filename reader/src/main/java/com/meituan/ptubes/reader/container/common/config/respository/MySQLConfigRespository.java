package com.meituan.ptubes.reader.container.common.config.respository;

import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;

public class MySQLConfigRespository {

    private volatile ProducerConfig producerConfig;
    private volatile StorageConfig storageConfig;

    public MySQLConfigRespository(ProducerConfig producerConfig,
        StorageConfig storageConfig) {
        this.producerConfig = producerConfig;
        this.storageConfig = storageConfig;
    }

    public ProducerConfig getProducerConfig() {
        return producerConfig;
    }

    public void setProducerConfig(ProducerConfig producerConfig) {
        this.producerConfig = producerConfig;
    }

    public StorageConfig getStorageConfig() {
        return storageConfig;
    }

    public void setStorageConfig(StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
    }

}
