package com.meituan.ptubes.sdk.cluster;



public interface IRdsCdcClusterMember {

    String getId();

    void join();

    void leave();

    boolean isConnected();
}
