package com.meituan.ptubes.reader.monitor.collector;

import com.meituan.ptubes.reader.monitor.metrics.ClientSessionStatMetrics;
import java.util.concurrent.atomic.AtomicReference;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;

public class ClientSessionStatMetricsCollector extends AbstractStatMetricsCollector<ClientSessionStatMetrics> {

	public ClientSessionStatMetricsCollector() {
		this.data = new ClientSessionStatMetrics();
	}

	public ClientSessionStatMetricsCollector(BinlogInfo initBinlogInfo) {
		this.data = new ClientSessionStatMetrics(initBinlogInfo);
	}

	@Override
	public void initStatMetrics(ClientSessionStatMetrics initData) {
		data.setWriteTaskName(initData.getWriteTaskName());
		data.setIp(initData.getIp());
		data.setPort(initData.getPort());

		data.setSubRequestCounter(initData.getSubRequestCounter());
		data.setSubRequestErrorCounter(initData.getSubRequestErrorCounter());
		data.setLastSubRequestCost(initData.getLastSubRequestCost());
		data.setFirstSubscriptionTime(initData.getFirstSubscriptionTime());
		data.setLastSubscriptionTime(initData.getLastSubscriptionTime());

		data.setGetRequestCounter(initData.getGetRequestCounter());
		data.setGetRequestErrorCounter(initData.getGetRequestErrorCounter());
		data.setLastGetRequestCost(initData.getLastGetRequestCost());
		data.setLastGetTime(initData.getLastGetTime());

		data.setLastGetBinlogInfo(initData.getLastGetBinlogInfo());

		data.setInboundTraffic(initData.getInboundTraffic());
		data.setOutboundTraffic(initData.getOutboundTraffic());
	}

	@Override
	public void resetStatMetrics() {
		data.setWriteTaskName("");
		data.setIp("");
		data.setPort(0);

		data.setSubRequestCounter(0);
		data.setSubRequestErrorCounter(0);
		data.setLastSubRequestCost(0);
		data.setFirstSubscriptionTime(0);
		data.setLastSubscriptionTime(0);

		data.setGetRequestCounter(0);
		data.setGetRequestErrorCounter(0);
		data.setLastGetRequestCost(0);
		data.setLastGetTime(0);

		data.getLastGetBinlogInfo().set(null);

		data.setInboundTraffic(0);
		data.setOutboundTraffic(0);
	}

	public ClientSessionStatMetricsCollector setWriteTaskName(String writeTaskName) {
		data.setWriteTaskName(writeTaskName);
		return this;
	}
	public String getWriteTaskName() {
		return data.getWriteTaskName();
	}

	public ClientSessionStatMetricsCollector setIp(String ip) {
		data.setIp(ip);
		return this;
	}
	public String getIp() {
		return data.getIp();
	}

	public ClientSessionStatMetricsCollector setPort(int port) {
		data.setPort(port);
		return this;
	}
	public int getPort() {
		return data.getPort();
	}

	public ClientSessionStatMetricsCollector setFirstSubscriptionTime(long firstSubscriptionTime) {
		data.setFirstSubscriptionTime(firstSubscriptionTime);
		return this;
	}
	public long getFirstSubscriptionTime() {
		return data.getFirstSubscriptionTime();
	}

	public void incrSubRequestCounter() {
		data.setSubRequestCounter(data.getSubRequestCounter() + 1);
	}
	public long getSubRequestCounter() {
		return data.getSubRequestCounter();
	}

	public void incrSubRequestErrorCounter() {
		data.setSubRequestErrorCounter(data.getSubRequestErrorCounter() + 1);
	}
	public long getSubRequestErrorCounter() {
		return data.getSubRequestErrorCounter();
	}

	public void setLastSubRequestCost(long lastSubRequestCost) {
		data.setLastSubRequestCost(lastSubRequestCost);
	}
	public long getLastSubRequestCost() {
		return data.getLastSubRequestCost();
	}

	public void setLastSubscriptionTime(long lastSubscriptionTime) {
		data.setLastSubscriptionTime(lastSubscriptionTime);
	}
	public long getLastSubscriptionTime() {
		return data.getLastSubscriptionTime();
	}

	public void incrGetRequestCounter() {
		data.setGetRequestCounter(data.getGetRequestCounter() + 1);
	}
	public long getGetRequestCounter() {
		return data.getGetRequestCounter();
	}

	public void incrGetRequestErrorCounter() {
		data.setGetRequestErrorCounter(data.getGetRequestErrorCounter() + 1);
	}
	public long getGetRequestErrorCounter() {
		return data.getGetRequestErrorCounter();
	}


	public void setLastGetRequestCost(long lastGetRequestCost) {
		data.setLastGetRequestCost(lastGetRequestCost);
	}
	public long getLastGetRequestCost() {
		return data.getLastGetRequestCost();
	}

	public void setLastGetTime(long lastGetTime) {
		data.setLastGetTime(lastGetTime);
	}
	public long getLastGetTime() {
		return data.getLastGetTime();
	}

	public void setLastGetBinlogInfo(BinlogInfo binlogInfo) {
		AtomicReference<BinlogInfo> lastGetBinlogInfo = data.getLastGetBinlogInfo();
		for (;;) {
			if (lastGetBinlogInfo.compareAndSet(lastGetBinlogInfo.get(), binlogInfo)) {
				break;
			}
		}
	}
	public BinlogInfo getLastGetBinlogInfo() {
		return data.getLastGetBinlogInfo().get();
	}

	public void accumulateInboundTraffic(long addition) {
		data.setInboundTraffic(data.getInboundTraffic() + addition);
	}
	public long getInboundTraffic() {
		return data.getInboundTraffic();
	}

	public void accumulateOutboundTraffic(long addition) {
		data.setOutboundTraffic(data.getOutboundTraffic() + addition);
	}
	public long getOutboundTraffic() {
		return data.getOutboundTraffic();
	}

	public long getRequestCounter() {
		return data.getSubRequestCounter() + data.getGetRequestCounter();
	}
	public long getRequestErrorCounter() {
		return data.getSubRequestErrorCounter() + data.getGetRequestErrorCounter();
	}
	public long getLastRequestCost() {
		return (data.getLastSubRequestCost() + data.getLastGetRequestCost()) >>> 1; // devided by 2
	}

	public void finishSubRequest(long subRequestCost) {
		this.incrSubRequestCounter();
		this.setLastSubRequestCost(subRequestCost);
		this.setLastSubscriptionTime(System.currentTimeMillis());
	}

	public void failSubRequest() {
		this.incrSubRequestCounter();
		this.incrSubRequestErrorCounter();
		this.setLastSubscriptionTime(System.currentTimeMillis());
	}

	public void finishGetRequest(long getRequestCost) {
		this.incrGetRequestCounter();
		this.setLastGetRequestCost(getRequestCost);
		this.setLastGetTime(System.currentTimeMillis());
	}

	public void failGetRequest() {
		this.incrGetRequestCounter();
		this.incrGetRequestErrorCounter();
		this.setLastGetTime(System.currentTimeMillis());
	}
}
