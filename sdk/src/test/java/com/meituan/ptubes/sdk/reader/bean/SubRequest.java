package com.meituan.ptubes.sdk.reader.bean;

import com.meituan.ptubes.sdk.checkpoint.MysqlCheckpoint;
import com.meituan.ptubes.sdk.model.PartitionClusterInfo;
import com.meituan.ptubes.sdk.model.ServiceGroupInfo;

public class SubRequest {

    private EncoderType codec = EncoderType.PROTOCOL_BUFFER; // default
    private MysqlCheckpoint rdsCdcCheckpoint;
    private ServiceGroupInfo serviceGroupInfo;
    private PartitionClusterInfo partitionClusterInfo;
    private boolean needDDL = true;
    private boolean needEndTransaction = true;

    public EncoderType getCodec() {
        return codec;
    }

    public void setCodec(EncoderType codec) {
        this.codec = codec;
    }

    public MysqlCheckpoint getRdsCdcCheckpoint() {
        return rdsCdcCheckpoint;
    }

    public void setRdsCdcCheckpoint(MysqlCheckpoint rdsCdcCheckpoint) {
        this.rdsCdcCheckpoint = rdsCdcCheckpoint;
    }

    public ServiceGroupInfo getServiceGroupInfo() {
        return serviceGroupInfo;
    }

    public void setServiceGroupInfo(ServiceGroupInfo serviceGroupInfo) {
        this.serviceGroupInfo = serviceGroupInfo;
    }

    public PartitionClusterInfo getPartitionClusterInfo() {
        return partitionClusterInfo;
    }

    public void setPartitionClusterInfo(PartitionClusterInfo partitionClusterInfo) {
        this.partitionClusterInfo = partitionClusterInfo;
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

    public enum EncoderType {
        PROTOCOL_BUFFER
    }
}
