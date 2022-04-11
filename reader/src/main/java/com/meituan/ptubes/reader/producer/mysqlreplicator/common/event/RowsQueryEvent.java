package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class RowsQueryEvent extends AbstractBinlogEventV4 {
	public static final int EVENT_TYPE = MySQLConstants.ROWS_QUERY_LOG_EVENT;

	private int length; // ignored
	private StringColumn queryText;

	public RowsQueryEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("queryText", queryText).toString();
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public StringColumn getQueryText() {
		return queryText;
	}

	public void setQueryText(StringColumn queryText) {
		this.queryText = queryText;
	}

}
