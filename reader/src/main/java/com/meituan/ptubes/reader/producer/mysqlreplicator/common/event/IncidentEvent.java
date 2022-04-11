package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

/**
 * Used to log an out of the ordinary event that occurred on the master.
 * It notifies the slave that something happened on the master that might
 * cause data to be in an inconsistent state.
 */
public final class IncidentEvent extends AbstractBinlogEventV4 {
	public static final int EVENT_TYPE = MySQLConstants.INCIDENT_EVENT;

	private int incidentNumber;
	private int messageLength;
	private StringColumn message;

	public IncidentEvent() {
	}

	public IncidentEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("incidentNumber", incidentNumber).append(
				"messageLength", messageLength).append("message", message).toString();
	}

	public int getIncidentNumber() {
		return incidentNumber;
	}

	public void setIncidentNumber(int incidentNumber) {
		this.incidentNumber = incidentNumber;
	}

	public int getMessageLength() {
		return messageLength;
	}

	public void setMessageLength(int messageLength) {
		this.messageLength = messageLength;
	}

	public StringColumn getMessage() {
		return message;
	}

	public void setMessage(StringColumn message) {
		this.message = message;
	}
}
