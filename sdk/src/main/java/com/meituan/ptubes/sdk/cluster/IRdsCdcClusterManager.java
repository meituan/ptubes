package com.meituan.ptubes.sdk.cluster;



public interface IRdsCdcClusterManager {

    void start() throws Exception;

    void shutdown();

    void updatePartitionNum(int partitionNum);
}
