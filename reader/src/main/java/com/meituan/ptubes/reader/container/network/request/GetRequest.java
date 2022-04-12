package com.meituan.ptubes.reader.container.network.request;

public class GetRequest extends Token {

	private int eventSize;
	private int eventNum;
	private long timeout;

	public GetRequest() {}

	public GetRequest(int eventSize, int eventNum, long timeout) {
		this.eventSize = eventSize;
		this.eventNum = eventNum;
		this.timeout = timeout;
	}

	public int getEventSize() {
		return eventSize;
	}

	public void setEventSize(int eventSize) {
		this.eventSize = eventSize;
	}

	public int getEventNum() {
		return eventNum;
	}

	public void setEventNum(int eventNum) {
		this.eventNum = eventNum;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("[ get | eventSize=").append(eventSize)
				.append(", eventNum=").append(eventNum)
				.append(", timeout=").append(timeout);
		return sb.toString();
	}
}
