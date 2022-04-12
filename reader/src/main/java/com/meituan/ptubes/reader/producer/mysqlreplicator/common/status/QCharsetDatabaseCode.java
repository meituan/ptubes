package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QCharsetDatabaseCode extends AbstractStatusVariable {
	public static final int TYPE = MySQLConstants.Q_CHARSET_DATABASE_CODE;

	private final int collationDatabase;

	public QCharsetDatabaseCode(int collationDatabase) {
		super(TYPE);
		this.collationDatabase = collationDatabase;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("collationDatabase", collationDatabase).toString();
	}

	public int getCollationDatabase() {
		return collationDatabase;
	}

	public static QCharsetDatabaseCode valueOf(XInputStream tis) throws IOException {
		final int collationDatabase = tis.readInt(2);
		return new QCharsetDatabaseCode(collationDatabase);
	}
}
