package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QFlags2Code extends AbstractStatusVariable {
	public static final int TYPE = MySQLConstants.Q_FLAGS2_CODE;

	private final int flags;

	public QFlags2Code(int flags) {
		super(TYPE);
		this.flags = flags;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("flags", flags).toString();
	}

	public int getFlags() {
		return flags;
	}

	public static QFlags2Code valueOf(XInputStream tis) throws IOException {
		return new QFlags2Code(tis.readInt(4));
	}
}
