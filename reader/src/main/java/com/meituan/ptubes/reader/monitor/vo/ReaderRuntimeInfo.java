package com.meituan.ptubes.reader.monitor.vo;

import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import com.meituan.ptubes.reader.container.common.vo.RedefinedToString;

public class ReaderRuntimeInfo extends RedefinedToString {

    private String readerTaskName;

    private com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig ProducerConfig;

    private StorageConfig storageConfig;

    public ReaderRuntimeInfo() {
    }

    public ReaderRuntimeInfo(String readerTaskName, ProducerConfig producerConfig, StorageConfig storageConfig) {
        this.readerTaskName = readerTaskName;
        ProducerConfig = producerConfig;
        this.storageConfig = storageConfig;
    }

    public String getReaderTaskName() {
        return readerTaskName;
    }

    public void setReaderTaskName(String readerTaskName) {
        this.readerTaskName = readerTaskName;
    }

    public com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig getProducerConfig() {
        return ProducerConfig;
    }

    public void setProducerConfig(ProducerConfig producerConfig) {
        ProducerConfig = producerConfig;
    }

    public StorageConfig getStorageConfig() {
        return storageConfig;
    }

    public void setStorageConfig(StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
    }
}
