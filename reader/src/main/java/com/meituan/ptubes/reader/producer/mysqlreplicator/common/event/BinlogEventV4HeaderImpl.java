package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class BinlogEventV4HeaderImpl implements BinlogEventV4Header {
	private long timestamp;
	private int eventType;
	private long serverId;
	private long eventLength;
	private long nextPosition;
	private int flags;
	private long replicatorInboundTS;

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("timestamp", timestamp).append("eventType", eventType).append(
				"serverId", serverId).append("eventLength", eventLength).append("nextPosition", nextPosition).append(
				"flags", flags).append("replicatorInboundTS", replicatorInboundTS).toString();
	}

	@Override public int getHeaderLength() {
		return 19;
	}

	@Override public long getPosition() {
		return this.nextPosition - this.eventLength;
	}

	@Override public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override public int getEventType() {
		return eventType;
	}

	public void setEventType(int eventType) {
		this.eventType = eventType;
	}

	@Override public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	@Override public long getEventLength() {
		return eventLength;
	}

	public void setEventLength(long eventLength) {
		this.eventLength = eventLength;
	}

	@Override public long getNextPosition() {
		return nextPosition;
	}

	public void setNextPosition(long nextPosition) {
		this.nextPosition = nextPosition;
	}

	@Override public int getFlags() {
		return flags;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	@Override public long getReplicatorInboundTS() {
		return replicatorInboundTS;
	}

	public void setReplicatorInboundTS(long replicatorInboundTS) {
		this.replicatorInboundTS = replicatorInboundTS;
	}
}
