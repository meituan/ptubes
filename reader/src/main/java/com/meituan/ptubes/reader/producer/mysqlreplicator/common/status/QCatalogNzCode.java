package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QCatalogNzCode extends AbstractStatusVariable {
	//
	public static final int TYPE = MySQLConstants.Q_CATALOG_NZ_CODE;

	//
	private final StringColumn catalogName;

	public QCatalogNzCode(StringColumn catalogName) {
		super(TYPE);
		this.catalogName = catalogName;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("catalogName", catalogName).toString();
	}

	public StringColumn getCatalogName() {
		return catalogName;
	}

	public static QCatalogNzCode valueOf(XInputStream tis) throws IOException {
		final int length = tis.readInt(1); // Length
		return new QCatalogNzCode(tis.readFixedLengthString(length));
	}
}
