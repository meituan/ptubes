package com.meituan.ptubes.sdk.checkpoint;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.meituan.ptubes.sdk.config.RdsCdcSourceType;


public class PartitionStorage {

    private int partitionNum;

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, defaultImpl = MysqlCheckpoint.class, property = "sourceType")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = MysqlCheckpoint.class, name = "MYSQL")
    })
    private BuffaloCheckpoint buffaloCheckpoint;

    private RdsCdcSourceType sourceType; // The MYSQL type must be left empty for forward compatibility before 1.5 is fully pushed

    public PartitionStorage() {

    }

    public PartitionStorage(int partitionNum, BuffaloCheckpoint buffaloCheckpoint) {
        this.partitionNum = partitionNum;
        this.buffaloCheckpoint = buffaloCheckpoint;
    }

    public PartitionStorage(int partitionNum, BuffaloCheckpoint buffaloCheckpoint, RdsCdcSourceType sourceType) {
        this.partitionNum = partitionNum;
        this.buffaloCheckpoint = buffaloCheckpoint;
        this.sourceType = sourceType;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
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

    @Override
    public String toString() {
        return new StringBuilder().append("PartitionStorage{partitionNum=")
            .append(partitionNum)
            .append(", buffaloCheckpoint=")
            .append(buffaloCheckpoint)
            .append("}")
            .toString();
    }
}
