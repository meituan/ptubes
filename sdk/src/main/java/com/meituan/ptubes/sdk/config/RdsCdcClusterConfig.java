package com.meituan.ptubes.sdk.config;

import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import com.meituan.ptubes.sdk.config.notification.IConfigChangeNotifier;

public class RdsCdcClusterConfig {
    private BuffaloCheckpoint initCheckpoint;

    private String zkAddr;

    private String taskName;

    private RdsCdcSourceType rdsCdcSourceType;

    IConfigChangeNotifier configChangeNotifier;

    private long numPartitions;

    private int zkSessionTimeoutMs;

    private int zkConnectionTimeoutMs;

    private int maxDisconnectThreshold;

    public BuffaloCheckpoint getInitCheckpoint() {
        return initCheckpoint;
    }

    public void setInitCheckpoint(BuffaloCheckpoint initCheckpoint) {
        this.initCheckpoint = initCheckpoint;
    }

    public String getZkAddr() {
        return zkAddr;
    }

    public void setZkAddr(String zkAddr) {
        this.zkAddr = zkAddr;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public RdsCdcSourceType getRdsCdcSourceType() {
        return rdsCdcSourceType;
    }

    public void setRdsCdcSourceType(RdsCdcSourceType rdsCdcSourceType) {
        this.rdsCdcSourceType = rdsCdcSourceType;
    }

    public IConfigChangeNotifier getConfigChangeNotifier() {
        return configChangeNotifier;
    }

    public void setConfigChangeNotifier(IConfigChangeNotifier configChangeNotifier) {
        this.configChangeNotifier = configChangeNotifier;
    }

    public long getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(long numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

    public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
        this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
    }

    public int getMaxDisconnectThreshold() {
        return maxDisconnectThreshold;
    }

    public void setMaxDisconnectThreshold(int maxDisconnectThreshold) {
        this.maxDisconnectThreshold = maxDisconnectThreshold;
    }
}
