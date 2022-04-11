package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

public final class LongColumn implements Column {
	private static final long serialVersionUID = -4109941053716659749L;

	public static final int MIN_VALUE = Integer.MIN_VALUE;
	public static final int MAX_VALUE = Integer.MAX_VALUE;

	private static final LongColumn[] CACHE = new LongColumn[255];

	static {
		for (int i = 0; i < CACHE.length; i++) {
			CACHE[i] = new LongColumn(i + Byte.MIN_VALUE);
		}
	}

	private final int value;

	private LongColumn(int value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override public Integer getValue() {
		return this.value;
	}

	public static final LongColumn valueOf(int value) {
		final int index = value - Byte.MIN_VALUE;
		return (index >= 0 && index < CACHE.length) ? CACHE[index] : new LongColumn(value);
	}
}
