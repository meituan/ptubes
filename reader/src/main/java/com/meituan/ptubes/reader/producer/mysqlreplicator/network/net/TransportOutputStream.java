package com.meituan.ptubes.reader.producer.mysqlreplicator.network.net;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XOutputStream;

public interface TransportOutputStream extends XOutputStream {

	void writePacket(Packet packet) throws IOException;
}
