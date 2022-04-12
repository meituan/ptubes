package com.meituan.ptubes.reader.container.common.config.service;

import com.meituan.ptubes.common.exception.SchemaParseException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;
import com.meituan.ptubes.common.exception.PtubesException;

public class ProducerConfigService implements IConfigService<ProducerConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerConfigService.class);
    private volatile ProducerConfig producerConfig;
    private final ReentrantReadWriteLock rwLock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private ConcurrentHashMap<String, ITableConfigChangeListener> tableConfigChangeListeners;

    public ProducerConfigService(
        ProducerConfig producerConfig
    ) {
        this.producerConfig = producerConfig;
        this.rwLock = new ReentrantReadWriteLock();
        this.readLock = rwLock.readLock();
        this.writeLock = rwLock.writeLock();
        this.tableConfigChangeListeners = new ConcurrentHashMap<>();
    }

    @Override
    public ProducerConfig getConfig(){
        return this.producerConfig;
    }

    @Override
    public void setConfig(ProducerConfig config) throws PtubesException {
        throw new PtubesException("Not support set producer config");
    }

    public boolean allow(String dbTableName) {
        return this.producerConfig.getTableConfigs()
            .isEmpty() || this.producerConfig.getTableConfigs()
            .containsKey(dbTableName);
    }

    public void setTableConfigs(Map<String, TableConfig> tableConfigMap) throws IOException, InterruptedException,
        SchemaParseException {
        ConcurrentHashMap<String, TableConfig> newTableMap = new ConcurrentHashMap<>(tableConfigMap);
        this.producerConfig.setTableConfigs(newTableMap);
        for (Map.Entry<String, ITableConfigChangeListener> entry : tableConfigChangeListeners.entrySet()) {
            entry.getValue().onChange(tableConfigMap);
            LOG.info("Notify {} tableConfig change success", entry.getKey());
        }
    }

    public void resetStoreSQLSwitch(boolean enableStroeSQL) {
        this.producerConfig.getProducerBaseConfig().setEnableStoreSQL(enableStroeSQL);
    }

    public Map<String, TableConfig> getTableConfigs() {
        return this.producerConfig.getTableConfigs();
    }

    public String getPartitionKey(String dbTableName) {
        if (this.producerConfig.getTableConfigs().containsKey(dbTableName)) {
            return this.producerConfig.getTableConfigs().get(dbTableName).getPartitionKey();
        } else {
            return "";
        }
    }

    public void addTableConfigChangeListener(String name, ITableConfigChangeListener listener) {
        this.tableConfigChangeListeners.put(name, listener);
    }

    public void rmTableConfigChangeListener(String name) {
        this.tableConfigChangeListeners.remove(name);
    }
}
