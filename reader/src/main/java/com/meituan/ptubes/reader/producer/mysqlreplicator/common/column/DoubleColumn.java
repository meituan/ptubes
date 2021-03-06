package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

public final class DoubleColumn implements Column {
	private static final long serialVersionUID = 7565759864274700531L;

	private final double value;

	private DoubleColumn(double value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override public Double getValue() {
		return this.value;
	}

	public static final DoubleColumn valueOf(double value) {
		return new DoubleColumn(value);
	}
}
