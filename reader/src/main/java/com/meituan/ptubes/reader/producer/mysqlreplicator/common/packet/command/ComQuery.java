package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.command;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XSerializer;

public class ComQuery extends AbstractCommandPacket {
	private static final long serialVersionUID = 1580858690926781520L;

	private StringColumn sql;

	public ComQuery() {
		super(MySQLConstants.COM_QUERY);
	}

	@Override public String toString() {
		return new ToStringBuilder(this).append("sql", sql).toString();
	}

	@Override public byte[] getPacketBody() throws IOException {
		final XSerializer ps = new XSerializer();
		ps.writeInt(this.command, 1);
		ps.writeFixedLengthString(this.sql);
		return ps.toByteArray();
	}

	public StringColumn getSql() {
		return sql;
	}

	public void setSql(StringColumn sql) {
		this.sql = sql;
	}
}
