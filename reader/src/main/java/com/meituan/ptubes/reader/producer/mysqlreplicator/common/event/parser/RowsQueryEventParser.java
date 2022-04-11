package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.RowsQueryEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class RowsQueryEventParser extends AbstractBinlogEventParser {
	public RowsQueryEventParser() {
		super(RowsQueryEvent.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final RowsQueryEvent event = new RowsQueryEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setLength(is.readInt(1));
		event.setQueryText(is.readFixedLengthString(is.available()));
		context.getEventListener().onEvents(event);
	}
}
