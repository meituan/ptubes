package com.meituan.ptubes.container.simulator;

import com.meituan.ptubes.reader.container.manager.ReaderTaskManager;
import com.meituan.ptubes.reader.container.manager.SessionManager;
import com.meituan.ptubes.reader.monitor.collector.AggregatedReadTaskStatMetricsCollector;

public class MockedReadTaskManager extends ReaderTaskManager {

    public MockedReadTaskManager(SessionManager sessionManager, AggregatedReadTaskStatMetricsCollector aggregatedReadTaskStatMetricsCollector) {
        super(sessionManager, aggregatedReadTaskStatMetricsCollector);
    }
}
