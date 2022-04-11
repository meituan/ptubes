package com.meituan.ptubes.reader.monitor.collector;

import com.meituan.ptubes.reader.monitor.metrics.ContainerStatMetrics;
import com.meituan.ptubes.reader.monitor.vo.ContainerInfo;

public class ContainerStatMetricsCollector extends AbstractStatMetricsCollector<ContainerStatMetrics> {

	public ContainerStatMetricsCollector() {
		this.data = new ContainerStatMetrics();
	}

	@Override
	public synchronized void initStatMetrics(ContainerStatMetrics initData) {
		if (initData == null) {
			resetStatMetrics();
		} else {
			data.getActiveConnectionNum().set(initData.getActiveConnectionNum().get());
			data.getIdleTimeoutConnectionNum().set(initData.getIdleTimeoutConnectionNum().get());
			data.getInboundTraffic().set(initData.getInboundTraffic().get());
			data.getOutboundTraffic().set(initData.getOutboundTraffic().get());
		}
	}

	@Override
	public synchronized void resetStatMetrics() {
		data.getActiveConnectionNum().set(0);
		data.getIdleTimeoutConnectionNum().set(0);
		data.getInboundTraffic().set(0);
		data.getOutboundTraffic().set(0);
	}

	public int incrActiveConnectionNum() {
		return data.getActiveConnectionNum()
			.incrementAndGet();
	}
	public int decrActiveConnectionNum() {
		return data.getActiveConnectionNum().decrementAndGet();
	}
	public int getActiveConnectionNum() {
		return data.getActiveConnectionNum().get();
	}

	public long incrIdleTimeoutConnectionNum() {
		return data.getIdleTimeoutConnectionNum().incrementAndGet();
	}
	public long getIdleTimeoutConnectionNum() {
		return data.getIdleTimeoutConnectionNum().get();
	}

	public long accumulateInboundTraffic(int addition) {
		return data.getInboundTraffic().addAndGet(addition);
	}
	public long getInboundTraffic() {
		return data.getInboundTraffic().get();
	}

	public long accumulateOutboundTraffic(int addition) {
		return data.getOutboundTraffic().addAndGet(addition);
	}
	public long getOutboundTraffic() {
		return data.getOutboundTraffic().get();
	}

	public ContainerInfo toContainerInfo() {
		return new ContainerInfo(this.getActiveConnectionNum(),
								 this.getIdleTimeoutConnectionNum(),
								 this.getInboundTraffic(),
								 this.getOutboundTraffic()
		);
	}

}
