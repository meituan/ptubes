package com.meituan.ptubes.reader.storage.common.event;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;


public class ErrorEvent extends EventInternalWritable {
	private EventType eventType;
	// no next event
	public static ErrorEvent NO_MORE_EVENT = new ErrorEvent(EventType.NO_MORE_EVENT);
	// not in cache
	public static ErrorEvent NOT_IN_BUFFER = new ErrorEvent(EventType.NOT_IN_BUFFER);
	// exception
	public static ErrorEvent EXCEPTION_EVENT = new ErrorEvent(EventType.EXCEPTION_EVENT);

	public ErrorEvent(EventType eventType) {
		this.eventType = eventType;
	}

	@Override
	public byte getVersion() {
		return 0;
	}

	@Override
	public EventInternalReadable reset(ByteBuffer buf, int position) {
		return null;
	}

	@Override
	public long getHeaderCrc() {
		return 0;
	}

	@Override
	public EventScanStatus scanEvent(boolean logErrors) {
		return null;
	}

	@Override
	public int getPayloadLength() {
		return 0;
	}

	@Override
	public long getValueCrc() {
		return 0;
	}

	@Override
	public long getCalculatedValueCrc() {
		return 0;
	}

	@Override
	public EventInternalWritable createCopy() {
		return null;
	}

	@Override
	public int writeTo(WritableByteChannel writeChannel, Encoding encoding) {
		return 0;
	}

	@Override
	public boolean isValid(boolean logErrors) {
		return false;
	}

	@Override
	protected HeaderScanStatus scanHeader(boolean logErrors) {
		return null;
	}

	@Override
	protected boolean isPartial() {
		return false;
	}

	@Override
	public HeaderScanStatus scanHeader() {
		return null;
	}

	@Override
	public EventScanStatus scanEvent() {
		return null;
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
		return null;
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
		return 0;
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
