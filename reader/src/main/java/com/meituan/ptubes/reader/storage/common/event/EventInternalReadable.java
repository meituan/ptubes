package com.meituan.ptubes.reader.storage.common.event;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


public abstract class EventInternalReadable extends PtubesEvent {
	public enum HeaderScanStatus {
		OK,
		ERR,
		PARTIAL,
	}

	public enum EventScanStatus {
		OK,
		ERR,
		PARTIAL,
	}

	public abstract byte getVersion();

	/** Resets this object to point to a ByteBuffer that holds an event */
	public abstract EventInternalReadable reset(ByteBuffer buf, int position);

	public abstract long getHeaderCrc();

	public abstract EventScanStatus scanEvent(boolean logErrors);

	public abstract int getPayloadLength();

	public abstract long getValueCrc();

	public abstract long getCalculatedValueCrc();

	public abstract EventInternalWritable createCopy();

	public abstract int writeTo(WritableByteChannel writeChannel, Encoding encoding);

	public abstract boolean isValid(boolean logErrors);

	protected abstract HeaderScanStatus scanHeader(boolean logErrors);

	protected abstract boolean isPartial();

	public abstract HeaderScanStatus scanHeader();

	public abstract EventScanStatus scanEvent();

}
