package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

public final class SetColumn implements Column {
	private static final long serialVersionUID = -5274295462701023264L;

	private final long value;

	private SetColumn(long value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override public Long getValue() {
		return this.value;
	}

	public static final SetColumn valueOf(long value) {
		return new SetColumn(value);
	}
}
