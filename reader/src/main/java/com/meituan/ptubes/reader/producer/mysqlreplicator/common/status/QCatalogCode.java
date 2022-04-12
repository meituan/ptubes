package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QCatalogCode extends AbstractStatusVariable {
	//
	public static final int TYPE = MySQLConstants.Q_CATALOG_CODE;

	//
	private final StringColumn catalogName;

	public QCatalogCode(StringColumn catalogName) {
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

	public static QCatalogCode valueOf(XInputStream tis) throws IOException {
		tis.readInt(1); // Length
		return new QCatalogCode(tis.readNullTerminatedString());
	}
}
