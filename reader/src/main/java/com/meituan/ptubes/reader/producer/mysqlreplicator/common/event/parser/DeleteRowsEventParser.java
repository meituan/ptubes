package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.DeleteRowsEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class DeleteRowsEventParser extends AbstractRowEventParser {

	public DeleteRowsEventParser() {
		super(DeleteRowsEvent.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final long tableId = is.readLong(6);
		final TableMapEvent tme = context.getTableMapEvent(tableId);
		if (this.rowEventFilter != null && !this.rowEventFilter.accepts(header, context, tme, tableId)) {
			skip(is, is.available());
			return;
		}

		final DeleteRowsEvent event = new DeleteRowsEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setTableId(tableId);
		event.setReserved(is.readInt(2));
		event.setColumnCount(is.readUnsignedLong());
		event.setUsedColumns(is.readBit(event.getColumnCount().intValue()));
		event.setRows(parseRows(is, tme, event));
		context.getEventListener().onEvents(event);
	}

	protected List<Row> parseRows(XInputStream is, TableMapEvent tme, DeleteRowsEvent dre) throws IOException {
		final List<Row> r = new LinkedList<Row>();
		while (is.available() > 0) {
			r.add(parseRow(is, tme, dre.getUsedColumns()));
		}
		return r;
	}
}
