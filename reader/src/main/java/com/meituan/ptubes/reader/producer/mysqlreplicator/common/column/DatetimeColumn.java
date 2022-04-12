package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLUtils;

public final class DatetimeColumn implements Column {

	private static final long serialVersionUID = 6444968242222031354L;

	private final java.util.Date value;
	private long longValue;

	private DatetimeColumn(java.util.Date value) {
		this.value = value;
		this.longValue = value.getTime();
	}

	private DatetimeColumn(long value) {
		this.longValue = value;
		this.value = MySQLUtils.toDatetime(value);
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override public java.util.Date getValue() {
		return this.value;
	}

	public long getLongValue() {
		return this.longValue;
	}

	public static final DatetimeColumn valueOf(java.util.Date value) {
		return new DatetimeColumn(value);
	}

	public static final DatetimeColumn valueOf(long value) {
		return new DatetimeColumn(value);
	}
}
