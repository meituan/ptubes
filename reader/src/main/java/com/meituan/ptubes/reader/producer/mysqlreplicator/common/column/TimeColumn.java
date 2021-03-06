package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

public final class TimeColumn implements Column {
	private static final long serialVersionUID = 2408833111678694298L;

	private final java.sql.Time value;

	private TimeColumn(java.sql.Time value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override public java.sql.Time getValue() {
		return this.value;
	}

	public static final TimeColumn valueOf(java.sql.Time value) {
		return new TimeColumn(value);
	}
}
