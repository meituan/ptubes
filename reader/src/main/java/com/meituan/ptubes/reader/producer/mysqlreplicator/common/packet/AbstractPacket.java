
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet;

import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Packet;

public abstract class AbstractPacket implements Packet {
	private static final long serialVersionUID = -2762990065527029085L;

	protected int length;
	protected int sequence;

	@Override public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	@Override public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}
}
