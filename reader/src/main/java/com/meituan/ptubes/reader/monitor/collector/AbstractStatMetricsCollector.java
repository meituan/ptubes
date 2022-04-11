package com.meituan.ptubes.reader.monitor.collector;

public abstract class AbstractStatMetricsCollector<DATA> {

	protected DATA data;

	abstract void initStatMetrics(DATA initData);

	abstract void resetStatMetrics();

}
