package com.meituan.ptubes.storage.mock;

import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.EventPart;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import java.nio.ByteBuffer;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;

public class MockedPtubesEvent extends PtubesEvent {
	private String tableName;
	private short partitionId;
	private EventType eventType;

	public MockedPtubesEvent(String tableName, short partitionId, EventType eventType) {
		this.tableName = tableName;
		this.partitionId = partitionId;
		this.eventType = eventType;
	}

	@Override
	public boolean isValid() {
		return false;
	}

	@Override
	public boolean isKeyNumber() {
		return false;
	}

	@Override
	public boolean isKeyString() {
		return false;
	}

	@Override
	public EventType getEventType() {
		return eventType;
	}

	@Override
	public String getFromIP() {
		return "";
	}

	@Override
	public long getTimestampInNS() {
		return 0;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public byte[] getBinlogInfoByte() {
		return new byte[0];
	}

	@Override
	public MySQLBinlogInfo getBinlogInfo() {
		return null;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public long getKey() {
		return 0;
	}

	@Override
	public byte[] getKeyBytes() {
		return new byte[0];
	}

	@Override
	public short getPartitionId() {
		return partitionId;
	}

	@Override
	public int getSchemaId() {
		return 0;
	}

	@Override
	public ByteBuffer getRawBytes() {
		return null;
	}

	@Override
	public byte[] getPayload() {
		return new byte[0];
	}

	@Override
	public EventPart getKeyPart() {
		return null;
	}
}
