package com.meituan.ptubes.reader.container.common.config.producer;

public class CdcExpermentalConfig {

    public static final CdcExpermentalConfig DEFAULT_CONFIG = new CdcExpermentalConfig();

    private long timeEventIntervalSeconds = 1;
    private int workerCount = 8;

    public long getTimeEventIntervalSeconds() {
        return timeEventIntervalSeconds;
    }

    public void setTimeEventIntervalSeconds(long timeEventIntervalSeconds) {
        this.timeEventIntervalSeconds = timeEventIntervalSeconds;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }
}
