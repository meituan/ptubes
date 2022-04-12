
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.StopEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class StopEventParser extends AbstractBinlogEventParser {

	public StopEventParser() {
		super(StopEvent.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final StopEvent event = new StopEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		context.getEventListener().onEvents(event);
	}
}
