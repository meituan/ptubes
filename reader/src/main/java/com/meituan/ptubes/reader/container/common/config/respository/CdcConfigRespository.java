package com.meituan.ptubes.reader.container.common.config.respository;

import com.meituan.ptubes.reader.container.common.config.producer.CdcConfig;
import com.meituan.ptubes.reader.container.common.config.producer.CdcExpermentalConfig;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerBaseConfig;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;
import com.meituan.ptubes.reader.container.common.config.storage.StorageConfig;
import java.util.Map;

public class CdcConfigRespository {

    private volatile ProducerBaseConfig producerBaseConfig;
    private volatile Map<String, TableConfig> subscriptionConfig;
    private volatile CdcConfig cdcConfig;
    private volatile StorageConfig storageConfig;
    private volatile CdcExpermentalConfig cdcExpermentalConfig;

    public CdcConfigRespository(ProducerBaseConfig producerBaseConfig,
        Map<String, TableConfig> subscriptionConfig, CdcConfig cdcConfig,
        StorageConfig storageConfig, CdcExpermentalConfig cdcExpermentalConfig) {
        this.producerBaseConfig = producerBaseConfig;
        this.subscriptionConfig = subscriptionConfig;
        this.cdcConfig = cdcConfig;
        this.storageConfig = storageConfig;
        this.cdcExpermentalConfig = cdcExpermentalConfig;
    }

    public ProducerBaseConfig getProducerBaseConfig() {
        return producerBaseConfig;
    }

    public void setProducerBaseConfig(ProducerBaseConfig producerBaseConfig) {
        this.producerBaseConfig = producerBaseConfig;
    }

    public Map<String, TableConfig> getSubscriptionConfig() {
        return subscriptionConfig;
    }

    public void setSubscriptionConfig(
        Map<String, TableConfig> subscriptionConfig) {
        this.subscriptionConfig = subscriptionConfig;
    }

    public CdcConfig getCdcConfig() {
        return cdcConfig;
    }

    public void setCdcConfig(CdcConfig cdcConfig) {
        this.cdcConfig = cdcConfig;
    }

    public StorageConfig getStorageConfig() {
        return storageConfig;
    }

    public void setStorageConfig(StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
    }

    public CdcExpermentalConfig getCdcExpermentalConfig() {
        return cdcExpermentalConfig;
    }

    public void setCdcExpermentalConfig(
        CdcExpermentalConfig cdcExpermentalConfig) {
        this.cdcExpermentalConfig = cdcExpermentalConfig;
    }
}
