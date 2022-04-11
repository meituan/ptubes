package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog;

import com.meituan.ptubes.reader.container.common.vo.GtidSet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;

public interface BinlogParserContext {
	boolean getChecksumEnabled();

	void setChecksumEnabled(boolean flag);

	String getBinlogFileName();

	BinlogEventListener getEventListener();

	TableMapEvent getTableMapEvent(long tableId);

	GtidSet getPrevGtidSet();
}
