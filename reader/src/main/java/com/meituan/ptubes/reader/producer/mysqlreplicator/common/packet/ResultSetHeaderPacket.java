package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Packet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XDeserializer;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XSerializer;

public class ResultSetHeaderPacket extends AbstractPacket {
	private static final long serialVersionUID = -5491186291875548645L;

	private UnsignedLong fieldCount;
	private UnsignedLong extra;

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("fieldCount", fieldCount).append("extra", extra).toString();
	}

	@Override public byte[] getPacketBody() {
		final XSerializer s = new XSerializer(32);
		s.writeUnsignedLong(this.fieldCount);
		if (this.extra != null) {
			s.writeUnsignedLong(this.extra);
		}
		return s.toByteArray();
	}

	public UnsignedLong getFieldCount() {
		return fieldCount;
	}

	public void setFieldCount(UnsignedLong fieldCount) {
		this.fieldCount = fieldCount;
	}

	public UnsignedLong getExtra() {
		return extra;
	}

	public void setExtra(UnsignedLong extra) {
		this.extra = extra;
	}

	public static ResultSetHeaderPacket valueOf(Packet packet) throws IOException {
		final XDeserializer d = new XDeserializer(packet.getPacketBody());
		final ResultSetHeaderPacket r = new ResultSetHeaderPacket();
		r.length = packet.getLength();
		r.sequence = packet.getSequence();
		r.fieldCount = d.readUnsignedLong();
		if (d.available() > 0) {
			r.extra = d.readUnsignedLong();
		}
		return r;
	}
}
