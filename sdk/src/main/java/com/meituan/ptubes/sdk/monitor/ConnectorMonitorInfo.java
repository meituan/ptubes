package com.meituan.ptubes.sdk.monitor;

import com.meituan.ptubes.sdk.checkpoint.BuffaloCheckpoint;
import java.util.HashMap;
import java.util.Map;

public class ConnectorMonitorInfo<C extends BuffaloCheckpoint> {
    private String taskName;
    private long startTimestamp;

    private FetchMonitorInfo fetchMonitorInfo;
    private Map<String, WorkMonitorInfo<C>> workMonitorInfoMap = new HashMap<>();

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public FetchMonitorInfo getFetchMonitorInfo() {
        return fetchMonitorInfo;
    }

    public void setFetchMonitorInfo(FetchMonitorInfo fetchMonitorInfo) {
        this.fetchMonitorInfo = fetchMonitorInfo;
    }

    public Map<String, WorkMonitorInfo<C>> getWorkMonitorInfoMap() {
        return workMonitorInfoMap;
    }

    public void setWorkMonitorInfoMap(Map<String, WorkMonitorInfo<C>> workMonitorInfoMap) {
        this.workMonitorInfoMap = workMonitorInfoMap;
    }

    @Override
    public String toString() {
        return "ConnectorMonitorInfo{" +
                "taskName='" + taskName + '\'' +
                ", startTimestamp=" + startTimestamp +
                ", fetchMonitorInfo=" + fetchMonitorInfo +
                ", workMonitorInfoMap=" + workMonitorInfoMap +
                '}';
    }
}
