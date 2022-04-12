package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QSQLModeCode extends AbstractStatusVariable {
	public static final int TYPE = MySQLConstants.Q_SQL_MODE_CODE;

	private final long sqlMode;

	public QSQLModeCode(long sqlMode) {
		super(TYPE);
		this.sqlMode = sqlMode;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("sqlMode", sqlMode).toString();
	}

	public long getSqlMode() {
		return sqlMode;
	}

	public static QSQLModeCode valueOf(XInputStream tis) throws IOException {
		return new QSQLModeCode(tis.readLong(8));
	}
}
