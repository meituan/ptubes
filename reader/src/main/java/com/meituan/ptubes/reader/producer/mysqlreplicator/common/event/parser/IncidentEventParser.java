package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.IncidentEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class IncidentEventParser extends AbstractBinlogEventParser {

	public IncidentEventParser() {
		super(IncidentEvent.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final IncidentEvent event = new IncidentEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setIncidentNumber(is.readInt(1));
		event.setMessageLength(is.readInt(1));
		if (event.getMessageLength() > 0) {
			event.setMessage(is.readFixedLengthString(event.getMessageLength()));
		}
		context.getEventListener().onEvents(event);
	}
}
