package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Packet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XDeserializer;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XSerializer;

public class EOFPacket extends AbstractPacket {
	private static final long serialVersionUID = 7001637720833705527L;

	public static final byte PACKET_MARKER = (byte) 0xFE;

	private int packetMarker;
	private int warningCount;
	private int serverStatus;

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("packetMarker", packetMarker).append("warningCount", warningCount)
				.append("serverStatus", serverStatus).toString();
	}

	@Override public byte[] getPacketBody() {
		final XSerializer s = new XSerializer(32);
		s.writeInt(this.packetMarker, 1);
		s.writeInt(this.warningCount, 2);
		s.writeInt(this.serverStatus, 2);
		return s.toByteArray();
	}

	public int getPacketMarker() {
		return packetMarker;
	}

	public void setPacketMarker(int packetMarker) {
		this.packetMarker = packetMarker;
	}

	public int getWarningCount() {
		return warningCount;
	}

	public void setWarningCount(int warningCount) {
		this.warningCount = warningCount;
	}

	public int getServerStatus() {
		return serverStatus;
	}

	public void setServerStatus(int serverStatus) {
		this.serverStatus = serverStatus;
	}

	public static EOFPacket valueOf(Packet packet) throws IOException {
		final XDeserializer d = new XDeserializer(packet.getPacketBody());
		final EOFPacket r = new EOFPacket();
		r.length = packet.getLength();
		r.sequence = packet.getSequence();
		r.packetMarker = d.readInt(1);
		r.warningCount = d.readInt(2);
		r.serverStatus = d.readInt(2);
		return r;
	}

	public static EOFPacket valueOf(int packetLength, int packetSequence, int packetMarker, XInputStream is)
			throws IOException {
		final EOFPacket r = new EOFPacket();
		r.length = packetLength;
		r.sequence = packetSequence;
		r.packetMarker = packetMarker;
		r.warningCount = is.readInt(2);
		r.serverStatus = is.readInt(2);
		return r;
	}
}
