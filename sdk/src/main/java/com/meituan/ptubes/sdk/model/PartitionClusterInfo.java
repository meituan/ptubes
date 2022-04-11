package com.meituan.ptubes.sdk.model;

import java.util.Set;



public class PartitionClusterInfo {
    private int partitionTotal;
    private Set<Integer> partitionSet;

    public PartitionClusterInfo() {

    }

    public PartitionClusterInfo(
        int partitionTotal,
        Set<Integer> partitionSet
    ) {
        this.partitionTotal = partitionTotal;
        this.partitionSet = partitionSet;
    }

    public void addPartition(int i) {
        this.partitionSet.add(i);
    }

    public void removePartition(int i) {
        this.partitionSet.remove(i);
    }

    public int getPartitionTotal() {
        return partitionTotal;
    }

    public void setPartitionTotal(int partitionTotal) {
        this.partitionTotal = partitionTotal;
    }

    public Set<Integer> getPartitionSet() {
        return partitionSet;
    }

    public void setPartitionSet(Set<Integer> partitionSet) {
        this.partitionSet = partitionSet;
    }

    @Override
    public String toString() {
        return "PartitionClusterInfo{" +
            "partitionTotal=" + partitionTotal +
            ", partitionSet=" + partitionSet +
            '}';
    }
}
