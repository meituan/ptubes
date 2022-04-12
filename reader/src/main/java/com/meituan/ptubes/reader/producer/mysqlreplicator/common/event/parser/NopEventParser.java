package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.HeartbeatEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;

public final class NopEventParser implements BinlogEventParser {

	@Override public int getEventType() {
		throw new UnsupportedOperationException();
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		skip(is, is.available());
		if(MySQLConstants.HEARTBEAT_LOG_EVENT == header.getEventType()) {
			context.getEventListener().onEvents(new HeartbeatEvent(header));
		}
	}

	@Override public void skip(XInputStream is, int skipLen) throws IOException {
		long skipBytes = is.skip(skipLen);
		if (skipBytes != skipLen) {
			LOG.warn("number of bytes({}) skipped is less than expected({})", skipBytes, skipLen);
		}
	}
}
