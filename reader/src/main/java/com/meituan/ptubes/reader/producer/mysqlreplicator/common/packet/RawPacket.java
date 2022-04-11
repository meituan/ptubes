package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet;

import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class RawPacket extends AbstractPacket {
	private static final long serialVersionUID = 4109090905397000303L;

	private byte[] packetBody;

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("length", length).append("sequence", sequence).toString();
	}

	@Override
	public byte[] getPacketBody() {
		return packetBody;
	}

	public void setPacketBody(byte[] packetBody) {
		this.packetBody = packetBody;
	}
}
