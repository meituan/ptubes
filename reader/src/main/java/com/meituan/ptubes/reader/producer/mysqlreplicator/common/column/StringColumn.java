package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

public final class StringColumn implements Column {
	private static final long serialVersionUID = 1009717372407166422L;

	private final byte[] value;

	private StringColumn(byte[] value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return new String(this.value);
	}

	@Override public byte[] getValue() {
		return this.value;
	}

	public static final StringColumn valueOf(byte[] value) {
		return new StringColumn(value);
	}
}
