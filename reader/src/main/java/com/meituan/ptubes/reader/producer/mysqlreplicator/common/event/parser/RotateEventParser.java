package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.RotateEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class RotateEventParser extends AbstractBinlogEventParser {

	public RotateEventParser() {
		super(RotateEvent.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final RotateEvent event = new RotateEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setBinlogPosition(is.readLong(8));
		event.setBinlogFileName(is.readFixedLengthString(is.available()));
		context.getEventListener().onEvents(event);
	}
}
