package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public abstract class AbstractBinlogEventParser implements BinlogEventParser {
	protected final int eventType;

	public AbstractBinlogEventParser(int eventType) {
		this.eventType = eventType;
	}

	@Override public final int getEventType() {
		return eventType;
	}

	@Override public void skip(XInputStream is, int skipLen) throws IOException {
		long skipBytes = is.skip(skipLen);
		if (skipBytes != skipLen) {
			LOG.warn("number of bytes({}) skipped is less than expected({})", skipBytes, skipLen);
		}
	}
}
