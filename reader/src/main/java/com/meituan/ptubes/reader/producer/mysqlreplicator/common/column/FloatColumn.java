package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

public final class FloatColumn implements Column {
	private static final long serialVersionUID = -890414733626452618L;

	private final float value;

	private FloatColumn(float value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override public Float getValue() {
		return this.value;
	}

	public static final FloatColumn valueOf(float value) {
		return new FloatColumn(value);
	}
}
