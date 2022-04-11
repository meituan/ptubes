
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.IntvarEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class IntvarEventParser extends AbstractBinlogEventParser {

	public IntvarEventParser() {
		super(IntvarEvent.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final IntvarEvent event = new IntvarEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setType(is.readInt(1));
		event.setValue(UnsignedLong.valueOf(is.readLong(8)));
		context.getEventListener().onEvents(event);
	}
}
