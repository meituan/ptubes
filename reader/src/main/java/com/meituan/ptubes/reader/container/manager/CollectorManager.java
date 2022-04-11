package com.meituan.ptubes.reader.container.manager;

import com.meituan.ptubes.common.utils.TimeUtil;
import com.meituan.ptubes.reader.container.common.utils.AppUtil;
import com.meituan.ptubes.reader.monitor.collector.AggregatedClientSessionStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.collector.AggregatedReadTaskStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.collector.ApplicationStatMetricsCollector;
import com.meituan.ptubes.reader.monitor.collector.ContainerStatMetricsCollector;
import com.meituan.ptubes.reader.container.common.AbstractLifeCycle;

/**
 * Manage monitoring metrics for individual components
 */
public class CollectorManager extends AbstractLifeCycle {

    private final ApplicationStatMetricsCollector applicationStatMetricsCollector;
    private final ContainerStatMetricsCollector containerStatMetricsCollector;
    private final AggregatedClientSessionStatMetricsCollector aggregatedClientSessionStatMetricsCollector;
    private final AggregatedReadTaskStatMetricsCollector aggregatedReadTaskStatMetricsCollector;

    public CollectorManager() {
        this.applicationStatMetricsCollector = new ApplicationStatMetricsCollector(
            AppUtil.getVersion(),
            TimeUtil.timeStr(System.currentTimeMillis())
        );
        this.containerStatMetricsCollector = new ContainerStatMetricsCollector();
        this.aggregatedClientSessionStatMetricsCollector = new AggregatedClientSessionStatMetricsCollector();
        this.aggregatedReadTaskStatMetricsCollector = new AggregatedReadTaskStatMetricsCollector();
    }

    @Override
    public boolean start() {
        return super.start();
    }

    @Override
    public boolean stop() {
        return super.stop();
    }

    public ApplicationStatMetricsCollector getApplicationStatMetricsCollector() {
        return applicationStatMetricsCollector;
    }

    public ContainerStatMetricsCollector getContainerStatMetricsCollector() {
        return containerStatMetricsCollector;
    }

    public AggregatedClientSessionStatMetricsCollector getAggregatedClientSessionStatMetricsCollector() {
        return aggregatedClientSessionStatMetricsCollector;
    }

    public AggregatedReadTaskStatMetricsCollector getAggregatedReadTaskStatMetricsCollector() {
        return aggregatedReadTaskStatMetricsCollector;
    }
}
