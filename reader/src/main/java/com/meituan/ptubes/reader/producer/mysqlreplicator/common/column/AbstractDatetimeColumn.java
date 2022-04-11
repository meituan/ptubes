package com.meituan.ptubes.reader.producer.mysqlreplicator.common.column;

import java.sql.Timestamp;

public abstract class AbstractDatetimeColumn implements DateTypeColumn {

	protected Timestamp timestampValue;

	public Timestamp getTimestampValue() {
		return this.timestampValue;
	}
}
