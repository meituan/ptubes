package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.BitColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Pair;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class UpdateRowsEventV2 extends AbstractRowEvent {
	public static final int EVENT_TYPE = MySQLConstants.UPDATE_ROWS_EVENT_V2;

	private int extraInfoLength;
	private byte[] extraInfo;
	private UnsignedLong columnCount;
	private BitColumn usedColumnsBefore;
	private BitColumn usedColumnsAfter;
	private List<Pair<Row>> rows;

	public UpdateRowsEventV2(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("tableId", tableId).append("reserved",
				reserved).append("extraInfoLength", extraInfoLength).append("extraInfo", extraInfo).append(
				"columnCount", columnCount).append("usedColumnsBefore", usedColumnsBefore).append("usedColumnsAfter",
				usedColumnsAfter).append("rows", rows).toString();
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
