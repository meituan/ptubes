package com.meituan.ptubes.reader.producer.mysqlreplicator.common.user;

import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class UserVariableDecimal extends AbstractUserVariable {
	public static final int TYPE = MySQLConstants.DECIMAL_RESULT;

	private final byte[] value;

	public UserVariableDecimal(byte[] value) {
		super(TYPE);
		this.value = value;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("value", value).toString();
	}

	@Override public byte[] getValue() {
		return this.value;
	}
}
