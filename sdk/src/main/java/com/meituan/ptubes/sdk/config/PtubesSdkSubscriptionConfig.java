package com.meituan.ptubes.sdk.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.model.DatabaseInfo;
import com.meituan.ptubes.sdk.model.ServiceGroupInfo;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class PtubesSdkSubscriptionConfig {

    // save point mode
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, defaultImpl = MysqlCheckpoint.class, property = "sourceType")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = MysqlCheckpoint.class, name = "MYSQL")
    })
    private BuffaloCheckpoint buffaloCheckpoint;

    private RdsCdcSourceType sourceType;

    // The appkey of the back-end reader cluster is used to go to mns for service discovery
    private String readerAppkey;

    // zk or router, only router mode in the first phase
    private String routerMode;

    // The address of the router service, the ZK address can be filled in here in the first phase, and the router address can be filled in here in the second phase
    private String routerAddress;

    // Each request, the number of events obtained in batches
    private int fetchBatchSize;

    // Each request, the timeout time for batch fetching
    private int fetchTimeoutMs;

    // Each request, the maximum number of bytes to be fetched in batches
    private int fetchMaxByteSize;

    // Do you need DDL events for each request?
    private boolean needDDL;

    // For each request, do you need an EndTransaction event?
    private boolean needEndTransaction;

    // number of shards
    private int partitionNum;

    // concerned table information
    private ServiceGroupInfo serviceGroupInfo;

    public PtubesSdkSubscriptionConfig() {
    }

    /**
     * for mysql
     * @param readerTaskName
     * @param sdkTaskName
     * @param zkAddress
     * @param databaseAndTable
     */
    public PtubesSdkSubscriptionConfig(String readerTaskName, String sdkTaskName, String zkAddress, String databaseAndTable) {
        this.buffaloCheckpoint = new MysqlCheckpoint(System.currentTimeMillis());
        this.sourceType = RdsCdcSourceType.MYSQL;
        this.readerAppkey = "";
        this.routerMode = "ZOOKEEPER";
        this.routerAddress = zkAddress;
        this.fetchBatchSize = 1000;
        this.fetchTimeoutMs = 50;
        this.fetchMaxByteSize = 1000000;
        this.needDDL = false;
        this.needEndTransaction = false;
        this.partitionNum = 11;

        Map<String, DatabaseInfo> databaseInfoMap = new HashMap<>();
        try {
            for (String dbTablePair : databaseAndTable.split(",")) {
                String db = dbTablePair.split("\\.")[0];
                String table = dbTablePair.split("\\.")[1];
                if (databaseInfoMap.containsKey(db)) {
                    databaseInfoMap.get(db).getTableNames().add(table);
                } else {
                    DatabaseInfo databaseInfo = new DatabaseInfo(db, new HashSet<>());
                    databaseInfoMap.put(db, databaseInfo);
                    databaseInfo.getTableNames().add(table);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot resolve databaseAndTable:" + databaseAndTable);
        }
        this.serviceGroupInfo = new ServiceGroupInfo(sdkTaskName, readerTaskName, new HashSet<>(databaseInfoMap.values()));
    }

    public enum RouterMode {
        ZOOKEEPER("zk"),
        ROUTER("router");

        private String desc;

        RouterMode(String desc) {
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }
    }

    public BuffaloCheckpoint getBuffaloCheckpoint() {
        return buffaloCheckpoint;
    }

    public void setBuffaloCheckpoint(BuffaloCheckpoint buffaloCheckpoint) {
        this.buffaloCheckpoint = buffaloCheckpoint;
    }

    public RdsCdcSourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(RdsCdcSourceType sourceType) {
        this.sourceType = sourceType;
    }

    public String getReaderAppkey() {
        return readerAppkey;
    }

    public void setReaderAppkey(String readerAppkey) {
        this.readerAppkey = readerAppkey;
    }

    public String getRouterMode() {
        return routerMode;
    }

    public void setRouterMode(String routerMode) {
        this.routerMode = routerMode;
    }

    public String getRouterAddress() {
        return routerAddress;
    }

    public void setRouterAddress(String routerAddress) {
        this.routerAddress = routerAddress;
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

    public ServiceGroupInfo getServiceGroupInfo() {
        return serviceGroupInfo;
    }

    public void setServiceGroupInfo(ServiceGroupInfo serviceGroupInfo) {
        this.serviceGroupInfo = serviceGroupInfo;
    }

    public boolean isNeedDDL() {
        return needDDL;
    }

    public void setNeedDDL(boolean needDDL) {
        this.needDDL = needDDL;
    }

    public boolean isNeedEndTransaction() {
        return needEndTransaction;
    }

    public void setNeedEndTransaction(boolean needEndTransaction) {
        this.needEndTransaction = needEndTransaction;
    }

    @Override
    public String toString() {
        return "RdsCdcClientSubscriptionConfig{" +
                "buffaloCheckpoint=" + buffaloCheckpoint +
                ", sourceType=" + sourceType +
                ", readerAppkey='" + readerAppkey + '\'' +
                ", routerMode='" + routerMode + '\'' +
                ", routerAddress='" + routerAddress + '\'' +
                ", fetchBatchSize=" + fetchBatchSize +
                ", fetchTimeoutMs=" + fetchTimeoutMs +
                ", fetchMaxByteSize=" + fetchMaxByteSize +
                ", needDDL=" + needDDL +
                ", needEndTransaction=" + needEndTransaction +
                ", partitionNum=" + partitionNum +
                ", serviceGroupInfo=" + serviceGroupInfo +
                '}';
    }
}
