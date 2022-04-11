package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.command;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XSerializer;

public class ComInitDBPacket extends AbstractCommandPacket {
	private static final long serialVersionUID = 449639496684376511L;

	private StringColumn databaseName;

	public ComInitDBPacket() {
		super(MySQLConstants.COM_INIT_DB);
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("databaseName", databaseName).toString();
	}

	@Override public byte[] getPacketBody() throws IOException {
		final XSerializer ps = new XSerializer();
		ps.writeInt(this.command, 1);
		ps.writeFixedLengthString(this.databaseName);
		return ps.toByteArray();
	}

	public StringColumn getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(StringColumn databaseName) {
		this.databaseName = databaseName;
	}
}
