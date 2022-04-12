package com.meituan.ptubes.reader.producer.mysqlreplicator.common.user;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.UserVariable;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public abstract class AbstractUserVariable implements UserVariable {
	protected final int type;

	public AbstractUserVariable(int type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).toString();
	}

	@Override public int getType() {
		return type;
	}
}
