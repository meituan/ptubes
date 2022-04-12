package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class BlobColumn implements Column {
	private static final long serialVersionUID = 756688909230132013L;

	private final byte[] value;

	private BlobColumn(byte[] value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("value", value).toString();
	}

	@Override public byte[] getValue() {
		return value;
	}

	public static final BlobColumn valueOf(byte[] value) {
		return new BlobColumn(value);
	}
}
