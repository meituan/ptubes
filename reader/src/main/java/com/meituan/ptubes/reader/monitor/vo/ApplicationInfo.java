package com.meituan.ptubes.reader.monitor.vo;

public class ApplicationInfo {

    private String startTime;
    private String version;

    public ApplicationInfo() {
    }

    public ApplicationInfo(
        String startTime,
        String version
    ) {
        this.startTime = startTime;
        this.version = version;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

}
