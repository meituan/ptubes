package com.meituan.ptubes.reader.monitor.metrics;

import java.util.concurrent.atomic.AtomicReference;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;

/* quote from java specification
* For the purposes of the Java programming language memory model, a single write to a non-volatile long or double value
* is treated as two separate writes: one to each 32-bit half. This can result in a situation where a thread sees the first 32 bits of a 64-bit value from one write, and the second 32 bits from another write.
*
* Writes and reads of volatile long and double values are always atomic.
* Writes to and reads of references are always atomic, regardless of whether they are implemented as
* 32-bit or 64-bit values.
*
* Some implementations may find it convenient to divide a single write action on a 64-bit long or double
* value into two write actions on adjacent 32-bit values. For efficiency's sake, this behavior is implementation-specific;
* an implementation of the Java Virtual Machine is free to perform writes to long and double values atomically or in
* two parts.
*
* Implementations of the Java Virtual Machine are encouraged to avoid splitting 64-bit values where possible.
* Programmers are encouraged to declare shared 64-bit values as volatile or synchronize their programs
* correctly to avoid possible complications.
* */
// For single-threaded modification, use volatile long to ensure atomic read and write, which can meet the requirements, and may read the old value when concurrent, use AtomicLong?
public class ClientSessionStatMetrics {

	private String writeTaskName = "";
	private String ip = ""; // Get hostname without dns
	private int port = 0;

	private volatile long inboundTraffic = 0;
	private volatile long outboundTraffic = 0;

	private volatile long subRequestCounter = 0;
	private volatile long subRequestErrorCounter = 0;
	private volatile long lastSubRequestCost = 0;
	private volatile long firstSubscriptionTime = 0;
	private volatile long lastSubscriptionTime = 0;

	private volatile long getRequestCounter = 0;
	private volatile long getRequestErrorCounter = 0;
	private volatile long lastGetRequestCost = 0;
	private volatile long lastGetTime = 0;

	private AtomicReference<BinlogInfo> lastGetBinlogInfo = new AtomicReference<>(null);

	// Calculated from the above statistics
	/*private volatile long requestCounter;
	private volatile long requestErrorCounter;
	private volatile long lastRequestCost;*/

	public ClientSessionStatMetrics() {
	}

	public ClientSessionStatMetrics(BinlogInfo binlogInfo) {
		this.lastGetBinlogInfo = new AtomicReference<>(binlogInfo);
	}

	public String getWriteTaskName() {
		return writeTaskName;
	}

	public void setWriteTaskName(String writeTaskName) {
		this.writeTaskName = writeTaskName;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public long getSubRequestCounter() {
		return subRequestCounter;
	}

	public void setSubRequestCounter(long subRequestCounter) {
		this.subRequestCounter = subRequestCounter;
	}

	public long getSubRequestErrorCounter() {
		return subRequestErrorCounter;
	}

	public void setSubRequestErrorCounter(long subRequestErrorCounter) {
		this.subRequestErrorCounter = subRequestErrorCounter;
	}

	public long getLastSubRequestCost() {
		return lastSubRequestCost;
	}

	public void setLastSubRequestCost(long lastSubRequestCost) {
		this.lastSubRequestCost = lastSubRequestCost;
	}

	public long getFirstSubscriptionTime() {
		return firstSubscriptionTime;
	}

	public void setFirstSubscriptionTime(long firstSubscriptionTime) {
		this.firstSubscriptionTime = firstSubscriptionTime;
	}

	public long getLastSubscriptionTime() {
		return lastSubscriptionTime;
	}

	public void setLastSubscriptionTime(long lastSubscriptionTime) {
		this.lastSubscriptionTime = lastSubscriptionTime;
	}

	public long getGetRequestCounter() {
		return getRequestCounter;
	}

	public void setGetRequestCounter(long getRequestCounter) {
		this.getRequestCounter = getRequestCounter;
	}

	public long getGetRequestErrorCounter() {
		return getRequestErrorCounter;
	}

	public void setGetRequestErrorCounter(long getRequestErrorCounter) {
		this.getRequestErrorCounter = getRequestErrorCounter;
	}

	public long getLastGetRequestCost() {
		return lastGetRequestCost;
	}

	public void setLastGetRequestCost(long lastGetRequestCost) {
		this.lastGetRequestCost = lastGetRequestCost;
	}

	public long getLastGetTime() {
		return lastGetTime;
	}

	public void setLastGetTime(long lastGetTime) {
		this.lastGetTime = lastGetTime;
	}

	public AtomicReference<BinlogInfo> getLastGetBinlogInfo() {
		return lastGetBinlogInfo;
	}

	public void setLastGetBinlogInfo(AtomicReference<BinlogInfo> lastGetBinlogInfo) {
		this.lastGetBinlogInfo = lastGetBinlogInfo;
	}

	public long getInboundTraffic() {
		return inboundTraffic;
	}

	public void setInboundTraffic(long inboundTraffic) {
		this.inboundTraffic = inboundTraffic;
	}

	public long getOutboundTraffic() {
		return outboundTraffic;
	}

	public void setOutboundTraffic(long outboundTraffic) {
		this.outboundTraffic = outboundTraffic;
	}
}
