package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QMasterDataWrittenCode extends AbstractStatusVariable {
	public static final int TYPE = MySQLConstants.Q_MASTER_DATA_WRITTEN_CODE;

	private final int value;

	public QMasterDataWrittenCode(int value) {
		super(TYPE);
		this.value = value;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("value", value).toString();
	}

	public int getValue() {
		return value;
	}

	public static QMasterDataWrittenCode valueOf(XInputStream tis) throws IOException {
		return new QMasterDataWrittenCode(tis.readInt(4));
	}
}
