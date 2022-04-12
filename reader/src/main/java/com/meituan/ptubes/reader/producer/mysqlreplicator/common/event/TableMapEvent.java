
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import java.util.Arrays;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.BitColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Metadata;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class TableMapEvent extends AbstractBinlogEventV4 {
	public static final int EVENT_TYPE = MySQLConstants.TABLE_MAP_EVENT;

	private long tableId;
	private int reserved;
	private int databaseNameLength;
	private StringColumn databaseName;
	private int tableNameLength;
	private StringColumn tableName;
	private UnsignedLong columnCount;
	private byte[] columnTypes;
	private UnsignedLong columnMetadataCount;
	private Metadata columnMetadata;
	private BitColumn columnNullabilities;

	public TableMapEvent() {
	}

	public TableMapEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("tableId", tableId).append("reserved",
				reserved).append("databaseNameLength", databaseNameLength).append("databaseName", databaseName).append(
				"tableNameLength", tableNameLength).append("tableName", tableName).append("columnCount", columnCount)
				.append("columnTypes", Arrays.toString(columnTypes)).append("columnMetadataCount", columnMetadataCount)
				.append("columnMetadata", columnMetadata).append("columnNullabilities", columnNullabilities).toString();
	}

	public TableMapEvent copy() {
		final TableMapEvent r = new TableMapEvent();
		r.setHeader(this.header);
		r.setTableId(this.tableId);
		r.setReserved(this.reserved);
		r.setDatabaseNameLength(this.databaseNameLength);
		r.setDatabaseName(this.databaseName);
		r.setTableNameLength(this.tableNameLength);
		r.setTableName(this.tableName);
		r.setColumnCount(this.columnCount);
		r.setColumnTypes(this.columnTypes);
		r.setColumnMetadataCount(this.columnMetadataCount);
		r.setColumnMetadata(this.columnMetadata);
		r.setColumnNullabilities(this.columnNullabilities);
		return r;
	}

	public long getTableId() {
		return tableId;
	}

	public void setTableId(long tableId) {
		this.tableId = tableId;
	}

	public int getReserved() {
		return reserved;
	}

	public void setReserved(int reserved) {
		this.reserved = reserved;
	}

	public int getDatabaseNameLength() {
		return databaseNameLength;
	}

	public void setDatabaseNameLength(int databaseNameLength) {
		this.databaseNameLength = databaseNameLength;
	}

	public StringColumn getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(StringColumn databaseName) {
		this.databaseName = databaseName;
	}

	public int getTableNameLength() {
		return tableNameLength;
	}

	public void setTableNameLength(int tableNameLength) {
		this.tableNameLength = tableNameLength;
	}

	public StringColumn getTableName() {
		return tableName;
	}

	public void setTableName(StringColumn tableName) {
		this.tableName = tableName;
	}

	public UnsignedLong getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(UnsignedLong columnCount) {
		this.columnCount = columnCount;
	}

	public byte[] getColumnTypes() {
		return columnTypes;
	}

	public void setColumnTypes(byte[] columnTypes) {
		this.columnTypes = columnTypes;
	}

	public UnsignedLong getColumnMetadataCount() {
		return columnMetadataCount;
	}

	public void setColumnMetadataCount(UnsignedLong columnMetadataCount) {
		this.columnMetadataCount = columnMetadataCount;
	}

	public Metadata getColumnMetadata() {
		return columnMetadata;
	}

	public void setColumnMetadata(Metadata columnMetadata) {
		this.columnMetadata = columnMetadata;
	}

	public BitColumn getColumnNullabilities() {
		return columnNullabilities;
	}

	public void setColumnNullabilities(BitColumn columnNullabilities) {
		this.columnNullabilities = columnNullabilities;
	}
}
