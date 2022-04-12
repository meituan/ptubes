
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.user;

import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class UserVariableReal extends AbstractUserVariable {
	//
	public static final int TYPE = MySQLConstants.REAL_RESULT;

	//
	private final double value;

	public UserVariableReal(double value) {
		super(TYPE);
		this.value = value;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("value", value).toString();
	}

	@Override public Double getValue() {
		return this.value;
	}
}
