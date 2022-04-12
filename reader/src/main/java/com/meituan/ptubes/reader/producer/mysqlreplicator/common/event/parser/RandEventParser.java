package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.RandEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class RandEventParser extends AbstractBinlogEventParser {

	public RandEventParser() {
		super(RandEvent.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final RandEvent event = new RandEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setRandSeed1(is.readLong(8));
		event.setRandSeed2(is.readLong(8));
		context.getEventListener().onEvents(event);
	}
}
