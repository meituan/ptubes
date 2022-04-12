package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class Row {
	private List<Column> columns;

	public Row() {
	}

	public Row(List<Column> columns) {
		this.columns = columns;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("columns", columns).toString();
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
}
