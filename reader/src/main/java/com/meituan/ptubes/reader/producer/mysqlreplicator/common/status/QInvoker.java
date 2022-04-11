package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QInvoker extends AbstractStatusVariable {
	//
	public static final int TYPE = MySQLConstants.Q_INVOKER;

	//
	private final StringColumn user;
	private final StringColumn host;

	public QInvoker(StringColumn user, StringColumn host) {
		super(TYPE);
		this.user = user;
		this.host = host;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("user", user).append("host", host).toString();
	}

	public StringColumn getUser() {
		return user;
	}

	public StringColumn getHost() {
		return host;
	}

	public static QInvoker valueOf(XInputStream tis) throws IOException {
		final int userLength = tis.readInt(1);
		final StringColumn user = tis.readFixedLengthString(userLength);
		final int hostLength = tis.readInt(1);
		final StringColumn host = tis.readFixedLengthString(hostLength);
		return new QInvoker(user, host);
	}
}
