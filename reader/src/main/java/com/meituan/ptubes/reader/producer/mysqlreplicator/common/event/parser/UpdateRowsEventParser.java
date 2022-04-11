package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Pair;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.UpdateRowsEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class UpdateRowsEventParser extends AbstractRowEventParser {

	public UpdateRowsEventParser() {
		super(UpdateRowsEvent.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final long tableId = is.readLong(6);
		final TableMapEvent tme = context.getTableMapEvent(tableId);
		if (this.rowEventFilter != null && !this.rowEventFilter.accepts(header, context, tme, tableId)) {
			skip(is, is.available());
			return;
		}

		final UpdateRowsEvent event = new UpdateRowsEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setTableId(tableId);
		event.setReserved(is.readInt(2));
		event.setColumnCount(is.readUnsignedLong());
		event.setUsedColumnsBefore(is.readBit(event.getColumnCount().intValue()));
		event.setUsedColumnsAfter(is.readBit(event.getColumnCount().intValue()));
		event.setRows(parseRows(is, tme, event));
		context.getEventListener().onEvents(event);
	}

	protected List<Pair<Row>> parseRows(XInputStream is, TableMapEvent tme, UpdateRowsEvent ure) throws IOException {
		final List<Pair<Row>> r = new LinkedList<Pair<Row>>();
		while (is.available() > 0) {
			final Row before = parseRow(is, tme, ure.getUsedColumnsBefore());
			final Row after = parseRow(is, tme, ure.getUsedColumnsAfter());
			r.add(new Pair<Row>(before, after));
		}
		return r;
	}
}
