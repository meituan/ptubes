package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

import java.util.Arrays;

/**
 * A descriptor event that is written to the beginning of the each binary log file.
 * This event is used as of MySQL 5.0; it supersedes START_EVENT_V3.
 */
public final class FormatDescriptionEvent extends AbstractBinlogEventV4 {
	public static final int EVENT_TYPE = MySQLConstants.FORMAT_DESCRIPTION_EVENT;

	private int binlogVersion;
	private StringColumn serverVersion;
	private long createTimestamp;
	private int headerLength;
	private byte[] eventTypes;

	public FormatDescriptionEvent() {
	}

	public FormatDescriptionEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("binlogVersion", binlogVersion).append(
				"serverVersion", serverVersion).append("createTimestamp", createTimestamp).append("headerLength",
				headerLength).append("eventTypes", Arrays.toString(eventTypes)).toString();
	}

	public int getBinlogVersion() {
		return binlogVersion;
	}

	public void setBinlogVersion(int binlogVersion) {
		this.binlogVersion = binlogVersion;
	}

	public StringColumn getServerVersion() {
		return serverVersion;
	}

	public void setServerVersion(StringColumn serverVersion) {
		this.serverVersion = serverVersion;
	}

	public long getCreateTimestamp() {
		return createTimestamp;
	}

	public void setCreateTimestamp(long createTimestamp) {
		this.createTimestamp = createTimestamp;
	}

	public int getHeaderLength() {
		return headerLength;
	}

	public void setHeaderLength(int headerLength) {
		this.headerLength = headerLength;
	}

	public byte[] getEventTypes() {
		return eventTypes;
	}

	public void setEventTypes(byte[] eventTypes) {
		this.eventTypes = eventTypes;
	}

	public boolean checksumEnabled() {
		if (checksumPossible()) {
			return this.eventTypes[this.eventTypes.length - 1] == 1;
		} else {
			return false;
		}

	}

	public boolean checksumPossible() {
		return true;
	}
}
