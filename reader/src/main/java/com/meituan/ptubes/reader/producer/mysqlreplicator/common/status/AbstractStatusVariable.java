package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.StatusVariable;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public abstract class AbstractStatusVariable implements StatusVariable {
	protected final int type;

	public AbstractStatusVariable(int type) {
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
