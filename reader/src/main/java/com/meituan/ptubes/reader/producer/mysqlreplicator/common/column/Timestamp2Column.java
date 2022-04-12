package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

public final class Timestamp2Column implements DateTypeColumn {
	private static final long serialVersionUID = 6334849626188321306L;

	private final java.sql.Timestamp value;
	private final String stringValue;

	private Timestamp2Column(java.sql.Timestamp value, String stringValue) {
		this.value = value;
		this.stringValue = stringValue;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override public java.sql.Timestamp getValue() {
		return this.value;
	}

	@Override public String getStringValue() {
		return stringValue;
	}

	public static final Timestamp2Column valueOf(java.sql.Timestamp value, String stringValue) {
		return new Timestamp2Column(value, stringValue);
	}
}
