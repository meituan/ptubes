package com.meituan.ptubes.reader.storage.common.event;

import java.nio.ByteBuffer;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;


public abstract class PtubesEvent {
	public enum SchemaDigestType {
		MD5,
		CRC32,
	}

	public final static int MD5_DIGEST_LEN = 16;
	public final static int CRC32_DIGEST_LEN = 4;

	/** Returns true iff the event points to a valid Databus event */
	public abstract boolean isValid();

	/** Returns true iff the getKey of the event is a numeric (long) */
	public abstract boolean isKeyNumber();

	/** Returns true iff the getKey is a string (byte sequence) */
	public abstract boolean isKeyString();

	// Event fields

	/** Returns the opcode of the data event; null for non-data events */
	public abstract EventType getEventType();

	@Deprecated
	public abstract String getFromIP();

	/** Returns the creation timestamp of the event in nanoseconds from Unix epoch */
	public abstract long getTimestampInNS();

	/**
	 * Returns the total size of the event binary serialization including header, metadata and payload.
	 */
	public abstract int size();

	public abstract byte[] getBinlogInfoByte();

	public abstract BinlogInfo getBinlogInfo();

	public abstract String getTableName();

	/** Returns getKey value for events with numeric keys; undefined for events with string keys. */
	public abstract long getKey();

	/** Returns the getKey value for events with string keys; undefined for events with numeric keys. */
	public abstract byte[] getKeyBytes();

	/**
	 * @return The partition id for the event.
	 */
	public abstract short getPartitionId();

	/**
	 * Returns a byte array with the hash id of the event serialization schema.
	 * <p> NOTE: this will most likely lead to a memory allocation. The preferred way to access the
	 * schema id is through. </p>
	 */
	@Deprecated
	public int getSchemaId() {
		return -1;
	}

	/**
	 * Returns a copy of the serialized event with the (implicitly) specified byte order.
	 */
	public abstract ByteBuffer getRawBytes();

	public abstract byte[] getPayload();

	/**
	 * @return Returns the DbusEventPart that corresponds to the getKey of the event (valid only if the getKey is of schema type).
	 * See isKeySchema();
	 * if this method is called and the getKey is not of schema type.
	 */
	@Deprecated
	public abstract EventPart getKeyPart();

}
