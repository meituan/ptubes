package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QTableMapForUpdateCode extends AbstractStatusVariable {
	public static final int TYPE = MySQLConstants.Q_TABLE_MAP_FOR_UPDATE_CODE;

	private final long tableMap;

	public QTableMapForUpdateCode(long tableMap) {
		super(TYPE);
		this.tableMap = tableMap;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("tableMap", tableMap).toString();
	}

	public long getTableMap() {
		return tableMap;
	}

	public static QTableMapForUpdateCode valueOf(XInputStream tis) throws IOException {
		return new QTableMapForUpdateCode(tis.readLong(8));
	}
}
