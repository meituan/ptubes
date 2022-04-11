package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

import org.joda.time.DateTime;

public final class DateColumn implements DateTypeColumn {

	private static final long serialVersionUID = 959710929844516680L;

	private final DateTime value;
	private final String stringValue;

	private DateColumn(DateTime value, String stringValue) {
		this.value = value;
		this.stringValue = stringValue;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override public DateTime getValue() {
		return this.value;
	}

	@Override public String getStringValue() {
		return stringValue;
	}

	public static final DateColumn valueOf(DateTime value, String stringValue) {
		return new DateColumn(value, stringValue);
	}
}
