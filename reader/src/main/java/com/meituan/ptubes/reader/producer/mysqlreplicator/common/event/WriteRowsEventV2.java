
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.BitColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class WriteRowsEventV2 extends AbstractRowEvent {
	public static final int EVENT_TYPE = MySQLConstants.WRITE_ROWS_EVENT_V2;

	private int extraInfoLength;
	private byte[] extraInfo;
	private UnsignedLong columnCount;
	private BitColumn usedColumns;
	private List<Row> rows;

	public WriteRowsEventV2(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("tableId", tableId).append("reserved",
				reserved).append("extraInfoLength", extraInfoLength).append("extraInfo", extraInfo).append(
				"columnCount", columnCount).append("usedColumns", usedColumns).append("rows", rows).toString();
	}

	public int getExtraInfoLength() {
		return extraInfoLength;
	}

	public void setExtraInfoLength(int extraInfoLength) {
		this.extraInfoLength = extraInfoLength;
	}

	public byte[] getExtraInfo() {
		return extraInfo;
	}

	public void setExtraInfo(byte[] extraInfo) {
		this.extraInfo = extraInfo;
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
