package com.meituan.ptubes.reader.producer.mysqlreplicator.network.net;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

public interface TransportInputStream extends XInputStream {
	Packet readPacket() throws IOException;

	public int currentPacketLength();

	public int currentPacketSequence();
}
