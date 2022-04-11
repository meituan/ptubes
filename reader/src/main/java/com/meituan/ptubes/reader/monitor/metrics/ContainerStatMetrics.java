package com.meituan.ptubes.reader.monitor.metrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ContainerStatMetrics {
	private AtomicInteger activeConnectionNum;
	private AtomicLong idleTimeoutConnectionNum;
	private AtomicLong inboundTraffic;
	private AtomicLong outboundTraffic;

	public ContainerStatMetrics() {
		this.activeConnectionNum = new AtomicInteger(0);
		this.idleTimeoutConnectionNum = new AtomicLong(0);
		this.inboundTraffic = new AtomicLong(0);
		this.outboundTraffic = new AtomicLong(0);
	}

	public AtomicInteger getActiveConnectionNum() {
		return activeConnectionNum;
	}

	public void setActiveConnectionNum(AtomicInteger activeConnectionNum) {
		this.activeConnectionNum = activeConnectionNum;
	}

	public AtomicLong getIdleTimeoutConnectionNum() {
		return idleTimeoutConnectionNum;
	}

	public void setIdleTimeoutConnectionNum(AtomicLong idleTimeoutConnectionNum) {
		this.idleTimeoutConnectionNum = idleTimeoutConnectionNum;
	}

	public AtomicLong getInboundTraffic() {
		return inboundTraffic;
	}

	public void setInboundTraffic(AtomicLong inboundTraffic) {
		this.inboundTraffic = inboundTraffic;
	}

	public AtomicLong getOutboundTraffic() {
		return outboundTraffic;
	}

	public void setOutboundTraffic(AtomicLong outboundTraffic) {
		this.outboundTraffic = outboundTraffic;
	}
}
