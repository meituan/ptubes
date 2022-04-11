package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.WriteRowsEventV2;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public class WriteRowsEventV2Parser extends AbstractRowEventParser {

	public WriteRowsEventV2Parser() {
		super(WriteRowsEventV2.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final long tableId = is.readLong(6);
		final TableMapEvent tme = context.getTableMapEvent(tableId);

		if (this.rowEventFilter != null && !this.rowEventFilter.accepts(header, context, tme, tableId)) {
			skip(is, is.available());
			return;
		}

		final WriteRowsEventV2 event = new WriteRowsEventV2(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setTableId(tableId);
		event.setReserved(is.readInt(2));
		event.setExtraInfoLength(is.readInt(2));
		if (event.getExtraInfoLength() > 2) {
			event.setExtraInfo(is.readBytes(event.getExtraInfoLength() - 2));
		}
		event.setColumnCount(is.readUnsignedLong()); // number of fields
		event.setUsedColumns(is.readBit(event.getColumnCount().intValue())); // Whether the field is used (bit operation, every 8 columns occupies one bit, rounded up)
		event.setRows(parseRows(is, tme, event));
		context.getEventListener().onEvents(event);
	}

	protected List<Row> parseRows(XInputStream is, TableMapEvent tme, WriteRowsEventV2 wre) throws IOException {
		final List<Row> r = new LinkedList<Row>();
		while (is.available() > 0) {
			r.add(parseRow(is, tme, wre.getUsedColumns()));
		}
		return r;
	}
}
