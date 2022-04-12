package com.meituan.ptubes.reader.monitor.metrics;

public class ApplicationStatMetrics {

    private volatile String version;
    private volatile String startTime;

    public ApplicationStatMetrics(String version, String startTime) {
        this.version = version;
        this.startTime = startTime;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }
}
