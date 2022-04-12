package com.meituan.ptubes.sdk.config;


import com.meituan.ptubes.sdk.config.notification.IConfigChangeNotifier;

public class FetchThreadConfig {

    private volatile String readerAppkey;

    private ReaderConnectionConfig readerConnectionConfig;

    private RdsCdcSourceType sourceType;

    private volatile int fetchBatchSize;

    private volatile int fetchTimeoutMs;

    private volatile int fetchMaxByteSize;

    private int partitionNum;

    private IConfigChangeNotifier configChangeNotifier;

    public String getReaderAppkey() {
        return readerAppkey;
    }

    public void setReaderAppkey(String readerAppkey) {
        this.readerAppkey = readerAppkey;
    }

    public RdsCdcSourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(RdsCdcSourceType sourceType) {
        this.sourceType = sourceType;
    }

    public ReaderConnectionConfig getReaderConnectionConfig() {
        return readerConnectionConfig;
    }

    public void setReaderConnectionConfig(ReaderConnectionConfig readerConnectionConfig) {
        this.readerConnectionConfig = readerConnectionConfig;
    }

    public int getFetchBatchSize() {
        return fetchBatchSize;
    }

    public void setFetchBatchSize(int fetchBatchSize) {
        this.fetchBatchSize = fetchBatchSize;
    }

    public int getFetchTimeoutMs() {
        return fetchTimeoutMs;
    }

    public void setFetchTimeoutMs(int fetchTimeoutMs) {
        this.fetchTimeoutMs = fetchTimeoutMs;
    }

    public int getFetchMaxByteSize() {
        return fetchMaxByteSize;
    }

    public void setFetchMaxByteSize(int fetchMaxByteSize) {
        this.fetchMaxByteSize = fetchMaxByteSize;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public IConfigChangeNotifier getConfigChangeNotifier() {
        return configChangeNotifier;
    }

    public void setConfigChangeNotifier(IConfigChangeNotifier configChangeNotifier) {
        this.configChangeNotifier = configChangeNotifier;
    }
}
