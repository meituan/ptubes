package com.meituan.ptubes.reader.container.common.config.producer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProducerConfig {
	private String readerTaskName;
	private ProducerBaseConfig producerBaseConfig;
	private volatile ConcurrentHashMap<String, TableConfig> tableConfigs;
	private volatile RdsConfig rdsConfig;

	public ProducerConfig() {
	}

	public ProducerConfig(String readerTaskName, ProducerBaseConfig producerBaseConfig, Map<String, TableConfig> tableConfigs,
						  RdsConfig rdsConfig
	) {
		this.readerTaskName = readerTaskName;
		this.producerBaseConfig = producerBaseConfig;
		this.tableConfigs = new ConcurrentHashMap<>(tableConfigs);
		this.rdsConfig = rdsConfig;
	}

	public String getReaderTaskName() {
		return readerTaskName;
	}

	public void setReaderTaskName(String readerTaskName) {
		this.readerTaskName = readerTaskName;
	}

	public ProducerBaseConfig getProducerBaseConfig() {
		return producerBaseConfig;
	}

	public void setProducerBaseConfig(ProducerBaseConfig producerBaseConfig) {
		this.producerBaseConfig = producerBaseConfig;
	}

	public ConcurrentHashMap<String, TableConfig> getTableConfigs() {
		return tableConfigs;
	}

	public void setTableConfigs(ConcurrentHashMap<String, TableConfig> tableConfigs) {
		this.tableConfigs = tableConfigs;
	}

	public RdsConfig getRdsConfig() {
		return rdsConfig;
	}

	public void setRdsConfig(RdsConfig rdsConfig) {
		this.rdsConfig = rdsConfig;
	}
}
