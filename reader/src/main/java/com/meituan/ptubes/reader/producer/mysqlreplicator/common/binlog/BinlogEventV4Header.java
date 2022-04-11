package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog;

public interface BinlogEventV4Header {

	int getHeaderLength();

	long getPosition();

	// unit: ms
	long getTimestamp();

	int getEventType();

	long getServerId();

	long getEventLength();

	long getNextPosition();

	int getFlags();

	long getReplicatorInboundTS();
}
