package com.meituan.ptubes.reader.monitor.collector;

import com.meituan.ptubes.common.utils.TimeUtil;
import com.meituan.ptubes.reader.monitor.vo.ApplicationInfo;
import com.meituan.ptubes.reader.monitor.metrics.ApplicationStatMetrics;

public class ApplicationStatMetricsCollector extends AbstractStatMetricsCollector<ApplicationStatMetrics> {

    public ApplicationStatMetricsCollector(String version, String startTime) {
        this.data = new ApplicationStatMetrics(version, startTime);
    }

    @Override
    public void initStatMetrics(ApplicationStatMetrics initData) {
        data.setStartTime(initData.getStartTime());
    }

    @Override
    public void resetStatMetrics() {
        data.setStartTime(TimeUtil.timeStr(System.currentTimeMillis()));
    }

    public void updateAppStartTime() {
        data.setStartTime(TimeUtil.timeStr(System.currentTimeMillis()));
    }
    public String getAppStartTime() {
        return data.getStartTime();
    }

    public void updateVersion(String version) {
        data.setVersion(version);
    }
    public String getVersion() {
        return data.getVersion();
    }

    public ApplicationInfo toApplicationInfo() {
        return new ApplicationInfo(this.getAppStartTime(), this.getVersion());
    }

}
