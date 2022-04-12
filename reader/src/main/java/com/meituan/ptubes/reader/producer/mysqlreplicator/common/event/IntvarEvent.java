package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class IntvarEvent extends AbstractBinlogEventV4 {
	public static final int EVENT_TYPE = MySQLConstants.INTVAR_EVENT;

	private int type;
	private UnsignedLong value;

	public IntvarEvent() {
	}

	public IntvarEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("type", type).append("value", value)
				.toString();
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public UnsignedLong getValue() {
		return value;
	}

	public void setValue(UnsignedLong value) {
		this.value = value;
	}
}
