package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Metadata;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class TableMapEventParser extends AbstractBinlogEventParser {
	private boolean reusePreviousEvent = true;

	public TableMapEventParser() {
		super(TableMapEvent.EVENT_TYPE);
	}

	public boolean isReusePreviousEvent() {
		return reusePreviousEvent;
	}

	public void setReusePreviousEvent(boolean reusePreviousEvent) {
		this.reusePreviousEvent = reusePreviousEvent;
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final long tableId = is.readLong(6);
		if (this.reusePreviousEvent && context.getTableMapEvent(tableId) != null) {
			skip(is, is.available());

			final TableMapEvent event = context.getTableMapEvent(tableId).copy();
			event.setHeader(header);
			event.setBinlogFilename(context.getBinlogFileName());
			context.getEventListener().onEvents(event);
			return;
		}

		final TableMapEvent event = new TableMapEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setTableId(tableId);
		event.setReserved(is.readInt(2));
		event.setDatabaseNameLength(is.readInt(1));
		event.setDatabaseName(is.readNullTerminatedString());
		event.setTableNameLength(is.readInt(1));
		event.setTableName(is.readNullTerminatedString());
		event.setColumnCount(is.readUnsignedLong());
		event.setColumnTypes(is.readBytes(event.getColumnCount().intValue()));
		event.setColumnMetadataCount(is.readUnsignedLong());
		event.setColumnMetadata(
				Metadata.valueOf(event.getColumnTypes(), is.readBytes(event.getColumnMetadataCount().intValue())));
		event.setColumnNullabilities(is.readBit(event.getColumnCount().intValue()));
		context.getEventListener().onEvents(event);
	}
}
