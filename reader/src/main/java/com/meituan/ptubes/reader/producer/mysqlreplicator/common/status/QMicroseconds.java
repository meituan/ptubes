package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QMicroseconds extends AbstractStatusVariable {
	public static final int TYPE = MySQLConstants.Q_MICROSECONDS;

	private final int startUsec;

	public QMicroseconds(int startUsec) {
		super(TYPE);
		this.startUsec = startUsec;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("startUsec", startUsec).toString();
	}

	public int getStartUsec() {
		return startUsec;
	}

	public static QMicroseconds valueOf(XInputStream tis) throws IOException {
		return new QMicroseconds(tis.readInt(3));
	}
}
