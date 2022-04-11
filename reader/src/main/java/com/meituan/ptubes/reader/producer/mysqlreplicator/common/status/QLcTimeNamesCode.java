package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QLcTimeNamesCode extends AbstractStatusVariable {
	public static final int TYPE = MySQLConstants.Q_LC_TIME_NAMES_CODE;

	private final int lcTimeNames;

	public QLcTimeNamesCode(int lcTimeNames) {
		super(TYPE);
		this.lcTimeNames = lcTimeNames;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("lcTimeNames", lcTimeNames).toString();
	}

	public int getLcTimeNames() {
		return lcTimeNames;
	}

	public static QLcTimeNamesCode valueOf(XInputStream tis) throws IOException {
		return new QLcTimeNamesCode(tis.readInt(2));
	}
}
