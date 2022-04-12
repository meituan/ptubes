package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.command;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.AbstractPacket;

public abstract class AbstractCommandPacket extends AbstractPacket {
	//
	private static final long serialVersionUID = -8046179372409111502L;

	//
	protected final int command;

	public AbstractCommandPacket(int command) {
		this.command = command;
	}

	public int getCommand() {
		return command;
	}
}
