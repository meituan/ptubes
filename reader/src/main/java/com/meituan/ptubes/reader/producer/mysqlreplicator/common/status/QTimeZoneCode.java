package com.meituan.ptubes.reader.producer.mysqlreplicator.common.status;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class QTimeZoneCode extends AbstractStatusVariable {
	public static final int TYPE = MySQLConstants.Q_TIME_ZONE_CODE;

	private final StringColumn timeZone;

	public QTimeZoneCode(StringColumn timeZone) {
		super(TYPE);
		this.timeZone = timeZone;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("timeZone", timeZone).toString();
	}

	public StringColumn getTimeZone() {
		return timeZone;
	}

	public static QTimeZoneCode valueOf(XInputStream tis) throws IOException {
		final int length = tis.readInt(1); // Length
		return new QTimeZoneCode(tis.readFixedLengthString(length));
	}
}
