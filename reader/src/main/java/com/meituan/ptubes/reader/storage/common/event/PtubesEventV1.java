package com.meituan.ptubes.reader.storage.common.event;

import java.nio.ByteBuffer;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.KeyTypeNotImplementedException;
import com.meituan.ptubes.common.exception.UnsupportedKeyException;
import com.meituan.ptubes.common.utils.BufferUtil;
import com.meituan.ptubes.common.utils.CRCUtil;
import com.meituan.ptubes.common.utils.IPUtil;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.common.utils.SerializeUtil;

/**
 * Attention: PtubesEventV1 only support MySQLBinlogInfo
 */
public class PtubesEventV1 extends EventSerializable implements Cloneable {
	public static final String MODULE = PtubesEventV1.class.getName();

	public static final byte VERSION = EventFactory.EVENT_V1;

	/**
	 * Serialization Format is :
	 * HEADER PART (52 bytes):
	 * Version (1 byte)               // 0 for V1 (historical), 2 for V2
	 * HeaderCrc (4 bytes)            // CRC to protect the header from being corrupted
	 *
	 * EventLength (4 bytes)
	 * EventType (1 bytes)           // event-type
	 * FromIp (8 bytes)
	 * KeyType (1 bytes)           // event-type
	 * BinlogInfo (58 bytes)             // Sequence number for the event window in which this event was generated
	 * Logical PartitionId (2 bytes)  // Short logical partition-id -> represents a logical partition of the physical stream
	 * NanoTimestamp (8 bytes)        // Time (in nanoseconds) at which the event was generated
	 * SchemaId (4 bytes)            // version
	 * BodyCrc (4 bytes)             // CRC to protect the value from being corrupted
	 *
	 * TableNameLength (2 bytes)
	 * TableName (TableNameLength bytes)
	 * KeySize (4 bytes)
	 * Key Bytes (k bytes for byte[] getKey)
	 * Value (N bytes)                // Serialized Event
	 */

	private static final int VERSION_OFFSET = 0;
	private static final int VERSION_BYTES = 1;
	private static final int HEADER_CRC_OFFSET = VERSION_OFFSET + VERSION_BYTES;
	private static final int HEADER_CRC_BYTES = 4;
	private static final int CRC_DEFAULT = 0;
	private static final int LENGTH_OFFSET = HEADER_CRC_OFFSET + HEADER_CRC_BYTES;
	private static final int LENGTH_BYTES = 4;
	private static final int EVENT_TYPE_OFFSET = LENGTH_OFFSET + LENGTH_BYTES;
	private static final int EVENT_TYPE_BYTES = 1;
	private static final int FROM_IP_OFFSET = EVENT_TYPE_OFFSET + EVENT_TYPE_BYTES;
	private static final int FROM_IP_BYTES = 8;
	private static final int KEY_TYPE_OFFSET = FROM_IP_OFFSET + FROM_IP_BYTES;
	private static final int KEY_TYPE_BYTES = 1;
	private static final int BINLOG_INFO_OFFSET = KEY_TYPE_OFFSET + KEY_TYPE_BYTES;
	private static final int BINLOG_INFO_BYTES = MySQLBinlogInfo.getSizeInByte();
	private static final int LOGICAL_PARTITION_ID_OFFSET = BINLOG_INFO_OFFSET + BINLOG_INFO_BYTES;
	private static final int LOGICAL_PARTITION_ID_BYTES = 2;
	private static final int TIMESTAMP_OFFSET = LOGICAL_PARTITION_ID_OFFSET + LOGICAL_PARTITION_ID_BYTES;
	private static final int TIMESTAMP_BYTES = 8;
	private static final int SCHEMA_ID_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_BYTES;
	private static final int SCHEMA_ID_BYTES = 4;
	private static final int BODY_CRC_OFFSET = SCHEMA_ID_OFFSET + SCHEMA_ID_BYTES;
	private static final int BODY_CRC_BYTES = 4;
	private static final int TABLE_NAME_LENGTH_OFFSET = BODY_CRC_OFFSET + BODY_CRC_BYTES;
	private static final int TABLE_NAME_LENGTH_BYTES = 2;
	private static final int KEY_LENGTH_BYTES = 4;
	private static final int FIXED_HEAD_BYTES = TABLE_NAME_LENGTH_OFFSET - LENGTH_OFFSET;
	private static final int LONG_KEY_BYTES = 8;

	// near-empty constructor that doesn't create a useful event
	public PtubesEventV1() {
		inited = false;
	}

	public PtubesEventV1(ByteBuffer buf, int position) {
		resetInternal(buf, position);
	}

	@Override
	public byte getVersion() {
		return (buf.get(position + VERSION_OFFSET));
	}

	@Override
	public long getHeaderCrc() {
		return BufferUtil.getUnsignedInt(buf, position + HEADER_CRC_OFFSET);
	}

	@Override
	public int size() {
		return (buf.getInt(position + LENGTH_OFFSET)); // length bytes
	}

	@Override
	public byte[] getBinlogInfoByte() {
		byte[] binlogInfo = new byte[BINLOG_INFO_BYTES];
		for (int i = 0; i < BINLOG_INFO_BYTES; i++) {
			binlogInfo[i] = buf.get(position + BINLOG_INFO_OFFSET + i);
		}
		return binlogInfo;
	}

	@Override
	public MySQLBinlogInfo getBinlogInfo() {
		MySQLBinlogInfo binlogInfo = new MySQLBinlogInfo();
		binlogInfo.decode(getBinlogInfoByte());
		return binlogInfo;
	}

	@Override
	public short getPartitionId() {
		return buf.getShort(position + LOGICAL_PARTITION_ID_OFFSET);
	}

	@Override
	public long getTimestampInNS() {
		return buf.getLong(position + TIMESTAMP_OFFSET);
	}

	@Override
	public int getSchemaId() {
		return buf.getInt(position + SCHEMA_ID_OFFSET);
	}

	@Override
	public long getValueCrc() {
		return BufferUtil.getUnsignedInt(buf, position + BODY_CRC_OFFSET);
	}

	public int getTableNameLength() {
		return buf.getShort(position + TABLE_NAME_LENGTH_OFFSET);
	}

	@Override
	public String getTableName() {
		int tableNameLength = getTableNameLength();
		byte[] tableName = new byte[tableNameLength];
		for (int i = 0; i < tableNameLength; i++) {
			tableName[i] = buf.get(position + TABLE_NAME_LENGTH_OFFSET + TABLE_NAME_LENGTH_BYTES + i);
		}
		return SerializeUtil.getString(tableName);
	}

	public int getKeyLengthOffset() {
		int tableNameLength = getTableNameLength();
		return TABLE_NAME_LENGTH_OFFSET + TABLE_NAME_LENGTH_BYTES + tableNameLength;
	}

	private int getKeyLength() {
		return buf.getInt(position + getKeyLengthOffset());
	}

	private int getKeyOffset() {
		return getKeyLengthOffset() + KEY_LENGTH_BYTES;
	}

	private int serializedTableNameLength() {
		return TABLE_NAME_LENGTH_BYTES + getTableNameLength();
	}

	private int serializedKeyLength() {
		if (isKeyString()) {
			return KEY_LENGTH_BYTES + getKeyLength();
		} else {
			return KEY_LENGTH_BYTES + LONG_KEY_BYTES;
		}
	}

	@Override
	public long getKey() {
		assert isKeyNumber();
		return buf.getLong(position + getKeyOffset());
	}

	@Override
	public byte[] getKeyBytes() {
		assert isKeyString();
		int keyLength = buf.getInt(position + getKeyLengthOffset());
		int keyOffset = getKeyOffset();
		byte[] dst = new byte[keyLength];
		for (int i = 0; i < keyLength; ++i) {
			dst[i] = buf.get(position + keyOffset + i);
		}
		return dst;
	}

	@Override
	public long getCalculatedValueCrc() {
		long calcValueCrc = CRCUtil.getChecksum(buf, position + TABLE_NAME_LENGTH_OFFSET, getPayloadLength());
		return calcValueCrc;
	}

	public int getValueOffset() {
		return getKeyOffset() + getKeyLength();
	}

	@Override
	public int getPayloadLength() {
		return (size() - TABLE_NAME_LENGTH_OFFSET);
	}

	public int getValueLength() {
		return (size() - (getKeyOffset() + getKeyLength()));
	}

	public ByteBuffer getValue() {
		ByteBuffer value = buf.asReadOnlyBuffer().order(buf.order());
		value.position(position + getValueOffset());
		value = value.slice().order(buf.order());
		int valueSize = getValueLength();
		value.limit(valueSize);
		value.rewind();
		return value;
	}

	
	@Override
	public byte[] getPayload() {
		int valueLength = getValueLength();
		int valueOffset = getValueOffset();
		byte[] dst = new byte[valueLength];

		
		int start = position + valueOffset;
		int end = start + valueLength;
		ByteBuffer payloadBuffer = buf.duplicate().order(buf.order());
		payloadBuffer.position(start);
		payloadBuffer.limit(end);
		payloadBuffer.get(dst, 0, valueLength);

		return dst;
	}

	public static int getCalculatedEventLength(ChangeEntry changeEntry)
			throws KeyTypeNotImplementedException, PtubesException, UnsupportedKeyException {
		// BinlogInfo Fixed length
		assert changeEntry instanceof MySQLChangeEntry;

		EventKey key = new EventKey(changeEntry.genEventKey());
		byte[] record = changeEntry.getSerializedRecord();
		int recordLength = record == null ? 0 : record.length;
		int tableNameLength = changeEntry.getTableName().length();
		switch (key.getKeyType()) {
		case LONG:
			return TABLE_NAME_LENGTH_OFFSET + TABLE_NAME_LENGTH_BYTES + tableNameLength
					+ KEY_LENGTH_BYTES + LONG_KEY_BYTES + recordLength;
		case STRING:
			return TABLE_NAME_LENGTH_OFFSET + TABLE_NAME_LENGTH_BYTES + tableNameLength
					+ KEY_LENGTH_BYTES + key.getStringKeyInBytes().length + recordLength;
		default:
			throw new KeyTypeNotImplementedException();
		}
	}

	public static int serializeEvent(MySQLChangeEntry mySQLChangeEntry, ByteBuffer serializationBuffer)
			throws PtubesException {
		try {
			EventKey key = new EventKey(mySQLChangeEntry.genEventKey());
			switch (key.getKeyType()) {
			case LONG:
				return serializeLongKeyEvent(key, mySQLChangeEntry, serializationBuffer);
			case STRING:
				return serializeStringKeyEvent(key, mySQLChangeEntry, serializationBuffer);
			default:
				throw new PtubesException("UnImplemented key type: " + key.getKeyType());
			}
		} catch (Exception e) {
			throw new PtubesException(e.getMessage());
		}

	}

	private static int serializeLongKeyEvent(EventKey key, MySQLChangeEntry mySQLChangeEntry, ByteBuffer serializationBuffer) {
		byte[] valueBytes = mySQLChangeEntry.getSerializedRecord();
		ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes);
		int payloadLen = (valueBuffer == null) ? valueBytes.length : valueBuffer.remaining();

		int startPosition = serializationBuffer.position();

		int eventLength = TABLE_NAME_LENGTH_OFFSET + TABLE_NAME_LENGTH_BYTES + mySQLChangeEntry.getTableName().length()
				+ KEY_LENGTH_BYTES + LONG_KEY_BYTES + payloadLen;

		serializationBuffer.put(VERSION).putInt(CRC_DEFAULT).putInt(eventLength).put(
				mySQLChangeEntry.getEventType().getCode()).putLong(mySQLChangeEntry.getFromServerIp()).put(
				EventKey.KeyType.LONG.getCode()).put(
				mySQLChangeEntry.getBinlogInfo().encode()).putShort(key.getLogicalPartitionId()).putLong(
				mySQLChangeEntry.getTimestamp()).putInt(mySQLChangeEntry.getSchemaId()).putInt(
				CRC_DEFAULT).putShort((short) mySQLChangeEntry.getTableName().length()).put(SerializeUtil.getBytes(mySQLChangeEntry.getTableName())).putInt(LONG_KEY_BYTES).putLong(
				key.getLongKey());
		if (valueBuffer != null) {
			// note. put will advance position. In the case of wrapped byte[] it is ok, in the case of
			// ByteBuffer this is actually a read only copy of the buffer passed in.
			serializationBuffer.put(valueBuffer);
		}

		int stopPosition = serializationBuffer.position();
		int valueLength = eventLength - TABLE_NAME_LENGTH_OFFSET;

		long valueCrc = CRCUtil.getChecksum(serializationBuffer, startPosition + TABLE_NAME_LENGTH_OFFSET, valueLength);
		BufferUtil.putUnsignedInt(serializationBuffer, startPosition + BODY_CRC_OFFSET, valueCrc);
		long headerCrc = CRCUtil.getChecksum(serializationBuffer, startPosition + LENGTH_OFFSET, FIXED_HEAD_BYTES);
		BufferUtil.putUnsignedInt(serializationBuffer, startPosition + HEADER_CRC_OFFSET, headerCrc);

		serializationBuffer.position(stopPosition);
		return (stopPosition - startPosition);
	}

	private static int serializeStringKeyEvent(EventKey key, MySQLChangeEntry mySQLChangeEntry,
			ByteBuffer serializationBuffer) {
		byte[] payLoad = mySQLChangeEntry.getSerializedRecord();
		ByteBuffer valueBuffer = null;
		if (payLoad != null) {
			valueBuffer = ByteBuffer.wrap(payLoad);
		}

		int payloadLen = (valueBuffer == null) ? 0 : valueBuffer.remaining();

		int startPosition = serializationBuffer.position();

		byte[] keyBytes = key.getStringKeyInBytes();
		int eventLength = TABLE_NAME_LENGTH_OFFSET + TABLE_NAME_LENGTH_BYTES + mySQLChangeEntry.getTableName().length()
				+ KEY_LENGTH_BYTES + keyBytes.length + payloadLen;

		serializationBuffer.put(VERSION).putInt(CRC_DEFAULT).putInt(eventLength).put(
				mySQLChangeEntry.getEventType().getCode()).putLong(mySQLChangeEntry.getFromServerIp()).put(
				EventKey.KeyType.STRING.getCode()).put(
				mySQLChangeEntry.getBinlogInfo().encode()).putShort(key.getLogicalPartitionId()).putLong(
				mySQLChangeEntry.getTimestamp()).putInt(mySQLChangeEntry.getSchemaId()).putInt(
				CRC_DEFAULT).putShort((short) mySQLChangeEntry.getTableName().length()).put(
				mySQLChangeEntry.getTableNameBytes()).putInt(keyBytes.length).put(keyBytes);
		if (valueBuffer != null) {
			serializationBuffer.put(valueBuffer);
		}

		int stopPosition = serializationBuffer.position();
		int valueLength = eventLength - TABLE_NAME_LENGTH_OFFSET;

		long valueCrc = CRCUtil.getChecksum(serializationBuffer, startPosition + TABLE_NAME_LENGTH_OFFSET, valueLength);
		BufferUtil.putUnsignedInt(serializationBuffer, startPosition + BODY_CRC_OFFSET, valueCrc);
		long headerCrc = CRCUtil.getChecksum(serializationBuffer, startPosition + LENGTH_OFFSET, FIXED_HEAD_BYTES);
		BufferUtil.putUnsignedInt(serializationBuffer, startPosition + HEADER_CRC_OFFSET, headerCrc);

		serializationBuffer.position(stopPosition);
		return (stopPosition - startPosition);
	}

	@Override
	public EventType getEventType() {
		return EventType.getByCode(buf.get(position + EVENT_TYPE_OFFSET));
	}

	@Override
	public String getFromIP() {
		return IPUtil.longToIp(buf.getLong(position + FROM_IP_OFFSET), false);
	}

	public byte getKeyType() {
		return buf.get(position + KEY_TYPE_OFFSET);
	}

	@Override
	public boolean isKeyString() {
		return EventKey.KeyType.STRING.getCode() == getKeyType();
	}

	@Override
	public boolean isKeyNumber() {
		return EventKey.KeyType.LONG.getCode() == getKeyType();
	}

	@Override
	public EventInternalReadable reset(ByteBuffer buf, int position) {
		if (buf.get(position) != VERSION) {
			verifyByteOrderConsistency(buf, "DbusEventV11.reset()");
			// If it is not the v10 version, use the v1 version to parse to ensure that the old version of the event sent by the relay can be compatible when the client is upgraded to the v10 version
			return PtubesEventV1Factory.createReadOnlyEventFromBuffer(buf, position);
		}
		resetInternal(buf, position);  // could optionally add "where" arg here, too
		return this;
	}

	public void applyHeaderCrc() {
		long headerCrc = CRCUtil.getChecksum(buf, position + LENGTH_OFFSET, FIXED_HEAD_BYTES);
		BufferUtil.putUnsignedInt(buf, position + HEADER_CRC_OFFSET, headerCrc);
	}

	@Override
	protected boolean isPartial() {
		return isPartial(true);
	}

	private boolean isPartial(boolean logErrors) {
		int size = size();
		if (size > (buf.limit() - position)) {
			if (logErrors) {
				LOG.error("partial event: size() = " + size + " buf_position=" + position + " limit = " + buf.limit()
						+ " (buf.limit()-position) = " + (buf.limit() - position));
			}
			return true;
		}
		return false;
	}

	/**
	 * @return PARTIAL if the event appears to be a partial event; ERR if the header is corrupted;
	 * OK if the event header is intact and the event appears to be complete
	 */
	@Override
	protected HeaderScanStatus scanHeader(boolean logErrors) {
		if (getVersion() != VERSION) {
			if (logErrors) {
				LOG.error("unknown version byte in header: " + getVersion());
			}
			return HeaderScanStatus.ERR;
		}

		int bytesInBuffer = buf.limit() - position;
		if (bytesInBuffer < HEADER_CRC_OFFSET) {
			// We can't even get to the header length
			return HeaderScanStatus.PARTIAL;
		}

		if (bytesInBuffer < LENGTH_OFFSET + FIXED_HEAD_BYTES) {
			return HeaderScanStatus.PARTIAL;
		}

		long calculatedHeaderCrc = CRCUtil.getChecksum(buf, position + LENGTH_OFFSET, FIXED_HEAD_BYTES);
		if (calculatedHeaderCrc != getHeaderCrc()) {
			if (logErrors) {
				LOG.error("Header CRC mismatch: ");
				LOG.error("getHeaderCrc() = " + getHeaderCrc());
				LOG.error("calculatedCrc = " + calculatedHeaderCrc);
			}
			return HeaderScanStatus.ERR;
		}
		return HeaderScanStatus.OK;
	}

	/**
	 * @return one of ERR / OK / PARTIAL
	 */
	@Override
	public EventScanStatus scanEvent(boolean logErrors) {
		HeaderScanStatus h = scanHeader(logErrors);

		if (h != HeaderScanStatus.OK) {
			if (logErrors) {
				LOG.error("HeaderScan error=" + h);
			}
			return (h == HeaderScanStatus.ERR ? EventScanStatus.ERR : EventScanStatus.PARTIAL);
		}

		//check if event is partial
		if (isPartial(logErrors)) {
			return EventScanStatus.PARTIAL;
		}

		int payloadLength = getPayloadLength();
		long calculatedValueCrc = getCalculatedValueCrc();

		long bodyCrc = getValueCrc();
		if (calculatedValueCrc != bodyCrc) {
			if (logErrors) {
				LOG.error("buf.order() = " + buf.order() + ", getValueCrc() = " + bodyCrc + ", crc.getValue() = "
						+ calculatedValueCrc + ", crc-ed block size = " + payloadLength);
			}
			return EventScanStatus.ERR;
		}

		return EventScanStatus.OK;
	}

	@Override
	public String toString() {
		if (null == buf) {
			return "buf=null";
		}

		boolean valid = true;

		try {
			valid = isValid(true);
		} catch (Exception ex) {
			LOG.error("PtubesEventV1.toString() : Got Exception while trying to validate the event ", ex);
			valid = false;
		}

		if (!valid) {
			StringBuilder sb = new StringBuilder("Position: ");
			sb.append(position);
			sb.append(", buf: ");
			sb.append(null != buf ? buf.toString() : "null");
			sb.append(", validity: false; hexDump:");
			if (null != buf && position >= 0) {
				sb.append(BufferUtil.hexdumpByteBufferContents(buf, position, 100));
			}

			return sb.toString();
		}

		StringBuilder sb = new StringBuilder(200);
		sb.append(";Position=").append(position).append(";Version=").append(getVersion()).append(";eventType=").append(
				getEventType()).append(";HeaderCrc=").append(getHeaderCrc()).append(";Length=").append(size()).append(
				";Key=");
		if (isKeyString()) {
			sb.append(new String(getKeyBytes()));
		} else {
			sb.append(getKey());
		}

		sb.append(";BinlogInfo=").append(getBinlogInfo()).append(";LogicalPartitionId=").append(getPartitionId())
				.append(";PhysicalPartitionId=").append(getPartitionId()).append(";Timestamp=").append(
				getTimestampInNS()).append(";SchemaId=").append(getSchemaId()).append(";ValueCrc=")
				.append(getValueCrc());
		return sb.toString();

	}

	@Override
	public EventPart getKeyPart() {
		throw new PtubesRunTimeException("V1 event does not support schema keys");
	}

	@Override
	public EventInternalWritable createCopy() {
		ByteBuffer cloned = ByteBuffer.allocate(size()).order(buf.order());
		cloned.put(getRawBytes());
		PtubesEventV1 c = new PtubesEventV1(cloned, 0);
		return c;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (obj instanceof PtubesEventV1) {
			PtubesEventV1 objEvent = (PtubesEventV1) obj;
			return (getHeaderCrc() == objEvent.getHeaderCrc() && getValueCrc() == objEvent.getValueCrc());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return ((int) getHeaderCrc() ^ (int) getValueCrc());
	}

}

