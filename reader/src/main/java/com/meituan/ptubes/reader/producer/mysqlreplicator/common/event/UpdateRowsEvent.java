package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;



import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.BitColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Pair;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class UpdateRowsEvent extends AbstractRowEvent {
	public static final int EVENT_TYPE = MySQLConstants.UPDATE_ROWS_EVENT;

	private UnsignedLong columnCount;
	private BitColumn usedColumnsBefore;
	private BitColumn usedColumnsAfter;
	private List<Pair<Row>> rows;

	public UpdateRowsEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("tableId", tableId).append("reserved",
				reserved).append("columnCount", columnCount).append("usedColumnsBefore", usedColumnsBefore).append(
				"usedColumnsAfter", usedColumnsAfter).append("rows", rows).toString();
	}

	public UnsignedLong getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(UnsignedLong columnCount) {
		this.columnCount = columnCount;
	}

	public BitColumn getUsedColumnsBefore() {
		return usedColumnsBefore;
	}

	public void setUsedColumnsBefore(BitColumn usedColumnsBefore) {
		this.usedColumnsBefore = usedColumnsBefore;
	}

	public BitColumn getUsedColumnsAfter() {
		return usedColumnsAfter;
	}

	public void setUsedColumnsAfter(BitColumn usedColumnsAfter) {
		this.usedColumnsAfter = usedColumnsAfter;
	}

	public List<Pair<Row>> getRows() {
		return rows;
	}

	public void setRows(List<Pair<Row>> rows) {
		this.rows = rows;
	}
}
