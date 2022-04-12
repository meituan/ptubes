package com.meituan.ptubes.sdk.config;

import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.model.PartitionClusterInfo;
import com.meituan.ptubes.sdk.model.ServiceGroupInfo;

public class ReaderConnectionConfig<C extends BuffaloCheckpoint> {

    private volatile PartitionClusterInfo partitionClusterInfo;

    private volatile ServiceGroupInfo serviceGroupInfo;

    private volatile C buffaloCheckpoint;

    private volatile boolean needEndTransaction;

    private volatile boolean needDDL;

    public void addPartition(int partitionId) {
        partitionClusterInfo.addPartition(partitionId);
    }

    public void removePartition(int partitionId) {
        partitionClusterInfo.removePartition(partitionId);
    }

    public PartitionClusterInfo getPartitionClusterInfo() {
        return partitionClusterInfo;
    }

    public void setPartitionClusterInfo(PartitionClusterInfo partitionClusterInfo) {
        this.partitionClusterInfo = partitionClusterInfo;
    }

    public ServiceGroupInfo getServiceGroupInfo() {
        return serviceGroupInfo;
    }

    public void setServiceGroupInfo(ServiceGroupInfo serviceGroupInfo) {
        this.serviceGroupInfo = serviceGroupInfo;
    }

    public C getBuffaloCheckpoint() {
        return buffaloCheckpoint;
    }

    public void setBuffaloCheckpoint(C buffaloCheckpoint) {
        this.buffaloCheckpoint = buffaloCheckpoint;
    }

    public boolean isNeedEndTransaction() {
        return needEndTransaction;
    }

    public void setNeedEndTransaction(boolean needEndTransaction) {
        this.needEndTransaction = needEndTransaction;
    }

    public boolean isNeedDDL() {
        return needDDL;
    }

    public void setNeedDDL(boolean needDDL) {
        this.needDDL = needDDL;
    }

    @Override
    public String toString() {
        return "ReaderConnectionConfig{" +
            "partitionClusterInfo=" + partitionClusterInfo +
            ", serviceGroupInfo=" + serviceGroupInfo +
            ", buffaloCheckpoint=" + buffaloCheckpoint +
            ", needEndTransaction=" + needEndTransaction +
            ", needDDL=" + needDDL +
            '}';
    }
}
