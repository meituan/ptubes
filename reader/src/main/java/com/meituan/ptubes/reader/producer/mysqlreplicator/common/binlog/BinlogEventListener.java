package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog;

public interface BinlogEventListener {

	void onEvents(BinlogEventV4 event);
}
