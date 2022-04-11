package com.meituan.ptubes.sdk.model.wrapper;

import java.util.List;
import com.meituan.ptubes.sdk.protocol.RdsPacket.RdsEvent;



public class AckEventWrapper {

    private List<RdsEvent> events;

    private int partitionTotal;

    private int currentPartition;

    /**
     * Used to ack data
     */
    private String batchUuid;

    public List<RdsEvent> getEvents() {
        return events;
    }

    public void setEvents(List<RdsEvent> events) {
        this.events = events;
    }

    public int getPartitionTotal() {
        return partitionTotal;
    }

    public void setPartitionTotal(int partitionTotal) {
        this.partitionTotal = partitionTotal;
    }

    public int getCurrentPartition() {
        return currentPartition;
    }

    public void setCurrentPartition(int currentPartition) {
        this.currentPartition = currentPartition;
    }

    public String getBatchUuid() {
        return batchUuid;
    }

    public void setBatchUuid(String batchUuid) {
        this.batchUuid = batchUuid;
    }
}
