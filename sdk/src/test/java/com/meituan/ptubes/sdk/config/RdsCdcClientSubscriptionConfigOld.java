package com.meituan.ptubes.sdk.config;

import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.model.ServiceGroupInfo;

/**
 * Emulate legacy configuration < version 1.5
 */
public class RdsCdcClientSubscriptionConfigOld {
    // save point mode
    private MysqlCheckpoint buffaloCheckpoint;

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

    public enum SourceType {
        MYSQL
    }

    public MysqlCheckpoint getBuffaloCheckpoint() {
        return buffaloCheckpoint;
    }

    public void setBuffaloCheckpoint(MysqlCheckpoint buffaloCheckpoint) {
        this.buffaloCheckpoint = buffaloCheckpoint;
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
        return "RdsCdcClientSubscriptionConfigV1{" +
                "buffaloCheckpoint=" + buffaloCheckpoint +
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
