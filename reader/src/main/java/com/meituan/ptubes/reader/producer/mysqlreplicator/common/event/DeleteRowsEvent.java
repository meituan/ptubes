package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.BitColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class DeleteRowsEvent extends AbstractRowEvent {

	public static final int EVENT_TYPE = MySQLConstants.DELETE_ROWS_EVENT;

	private UnsignedLong columnCount;
	private BitColumn usedColumns;
	private List<Row> rows;

	public DeleteRowsEvent() {
	}

	public DeleteRowsEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("tableId", tableId).append("reserved",
				reserved).append("columnCount", columnCount).append("usedColumns", usedColumns).append("rows", rows)
				.toString();
	}

	public UnsignedLong getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(UnsignedLong columnCount) {
		this.columnCount = columnCount;
	}

	public BitColumn getUsedColumns() {
		return usedColumns;
	}

	public void setUsedColumns(BitColumn usedColumns) {
		this.usedColumns = usedColumns;
	}

	public List<Row> getRows() {
		return rows;
	}

	public void setRows(List<Row> rows) {
		this.rows = rows;
	}
}
