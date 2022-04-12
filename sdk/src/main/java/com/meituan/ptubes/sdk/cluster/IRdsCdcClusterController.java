package com.meituan.ptubes.sdk.cluster;

import org.apache.helix.ControllerChangeListener;



public interface IRdsCdcClusterController extends ControllerChangeListener {

    void startController();

    void stopController();

    void pauseController();

    void resumeController();

    void updatePartitionNum(int partitionNum);

    boolean isLeader();
}
