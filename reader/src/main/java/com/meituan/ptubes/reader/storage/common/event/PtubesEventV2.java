package com.meituan.ptubes.reader.storage.common.event;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.KeyTypeNotImplementedException;
import com.meituan.ptubes.common.exception.UnsupportedKeyException;
import com.meituan.ptubes.common.utils.BufferUtil;
import com.meituan.ptubes.common.utils.CRCUtil;
import com.meituan.ptubes.common.utils.IPUtil;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.storage.mem.buffer.BinlogInfoFactory;
import com.meituan.ptubes.common.utils.SerializeUtil;

/**
 * Attention: In the future, PtubesEventV2 may have different interfaces from PtubesEventV1, or add an adapter between different versions of Event to unify the interface to the outside world
 */
public class PtubesEventV2 extends EventSerializable implements Cloneable {

    /**
     * If it is C, you should consider the field order of the parsed object and the impact of memory alignment
     * Serialization Format is :
     *
     * Version (1 byte)                 // 0 for V1 (historical), 2 for V2
     * HeaderCrc (4 bytes)              // CRC to protect the header from being corrupted
     *
     * ============================ HEADER PART (variable size) ==========================
     * // fixed header part
     * EventLength (4 bytes)            // length(header + body)
     * SourceType (1 byte)
     * EventType (1 bytes)              // event-type
     * FromIp (8 bytes)                 // IPV6?
     * KeyType (1 bytes)                // key-type (Deprecate)
     * Logical PartitionId (2 bytes)    // Short logical partition-id -> represents a logical partition of the physical stream
     * NanoTimestamp (8 bytes)          // Time (in nanoseconds) at which the event was generated
     * BodyCrc (4 bytes)                // CRC to protect the value from being corrupted
     *
     * // variable header part
     * BinlogInfoLength (1 byte) // Convenient for subsequent expansion of binlogInfo
     * BinlogInfoBody (58 bytes for MySQL, 49 bytes for Blade)
     * ExtraFlagsLength (4 bytes) // when Version=V2, extraHanderLength === 0
     * ExtraFlags // format to be determined
     *
     * ============================ BODY PART (variable size) ==========================
     * TableNameLength (2 bytes)
     * TableName (TableNameLength bytes)
     * KeySize (4 bytes) (Deprecate)
     * Key Bytes (k bytes for byte[] getKey) (Deprecate)
     *
     * Value (N bytes)                // Serialized Event
     */

    private static final byte VERSION = EventFactory.EVENT_V2;

    // fixed header part offset
    private static final int CRC_DEFAULT = 0;

    private static final int VERSION_OFFSET = 0;
    private static final int VERSION_BYTES = 1;
    private static final int HEADER_CRC_OFFSET = VERSION_OFFSET + VERSION_BYTES;
    private static final int HEADER_CRC_BYTES = 4;

    private static final int EVENT_LENGTH_OFFSET = HEADER_CRC_OFFSET + HEADER_CRC_BYTES;
    private static final int EVENT_LENGTH_BYTES = 4;
    private static final int SOURCE_TYPE_OFFSET = EVENT_LENGTH_OFFSET + EVENT_LENGTH_BYTES;
    private static final int SOURCE_TYPE_BYTES = 1;
    private static final int EVENT_TYPE_OFFSET = SOURCE_TYPE_OFFSET + SOURCE_TYPE_BYTES;
    private static final int EVENT_TYPE_BYTES = 1;
    private static final int FROM_IP_OFFSET = EVENT_TYPE_OFFSET + EVENT_TYPE_BYTES;
    private static final int FROM_IP_BYTES = 8;
    private static final int KEY_TYPE_OFFSET = FROM_IP_OFFSET + FROM_IP_BYTES;
    private static final int KEY_TYPE_BYTES = 1;
    private static final int LOGICAL_PARTITION_ID_OFFSET = KEY_TYPE_OFFSET + KEY_TYPE_BYTES;
    private static final int LOGICAL_PARTITION_ID_BYTES = 2;
    private static final int TIMESTAMP_OFFSET = LOGICAL_PARTITION_ID_OFFSET + LOGICAL_PARTITION_ID_BYTES;
    private static final int TIMESTAMP_BYTES = 8;
    private static final int BODY_CRC_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_BYTES;
    private static final int BODY_CRC_BYTES = 4;

    private static final int BINLOG_INFO_LENGTH_OFFSET = BODY_CRC_OFFSET + BODY_CRC_BYTES;
    private static final int BINLOG_INFO_LENGTH_BYTES = 1;
    private static final int BINLOG_INFO_OFFSET = BINLOG_INFO_LENGTH_OFFSET + BINLOG_INFO_LENGTH_BYTES;
    private static final int FIXED_HEADER_BYTES = BINLOG_INFO_OFFSET;
    private static final int EXTRA_FLAGS_LENGTH_BYTES = 4;
    private static final int TABLE_NAME_LENGTH_BYTES = 2;
    private static final int KEY_LENGTH_BYTES = 4;
    private static final int LONG_KEY_BYTES = 8;

    public static int getCalculatedEventLength(ChangeEntry changeEntry)
        throws KeyTypeNotImplementedException, PtubesException, UnsupportedKeyException {
        EventKey key = new EventKey(changeEntry.genEventKey());

        byte[] record = changeEntry.getSerializedRecord();
        int recordLength = Objects.isNull(record) ? 0 : record.length;
        int binlogInfoLength = changeEntry.getBinlogInfo().getLength();
        // Attention: reserved extension extraFlags, no encoding yet
        int extraFlagsLength = 0;
        int tableNameLength = changeEntry.getTableName().length();

        switch (key.getKeyType()) {
            case LONG:
                return BINLOG_INFO_OFFSET + binlogInfoLength + EXTRA_FLAGS_LENGTH_BYTES + extraFlagsLength + TABLE_NAME_LENGTH_BYTES + tableNameLength
                    + KEY_LENGTH_BYTES + LONG_KEY_BYTES + recordLength;
            case STRING:
                return BINLOG_INFO_OFFSET + binlogInfoLength + EXTRA_FLAGS_LENGTH_BYTES + extraFlagsLength + TABLE_NAME_LENGTH_BYTES + tableNameLength
                    + KEY_LENGTH_BYTES + key.getStringKeyInBytes().length + recordLength;
            default:
                throw new KeyTypeNotImplementedException();
        }
    }

    public static int serializeEvent(ChangeEntry changeEntry, ByteBuffer serializationBuffer) throws PtubesException {
        try {
            EventKey key = new EventKey(changeEntry.genEventKey());
            switch (key.getKeyType()) {
                case LONG:
                    return serializeLongKeyEvent(key, changeEntry, serializationBuffer);
                case STRING:
                    return serializeStringKeyEvent(key, changeEntry, serializationBuffer);
                default:
                    throw new PtubesException("UnImplemented key type: " + key.getKeyType());
            }
        } catch (Exception e) {
            throw new PtubesException(e);
        }
    }

    private static int serializeLongKeyEvent(EventKey key, ChangeEntry changeEntry, ByteBuffer serializationBuffer) {
        byte[] valueBytes = changeEntry.getSerializedRecord();
        byte[] extraFlagsBytes = changeEntry.getSerializedExtraFlags();

        int payloadLen = valueBytes.length;
        int extraFlagsLen = extraFlagsBytes.length;
        int binlogLen = changeEntry.getBinlogInfo().getLength();
        int tableNameLen = changeEntry.getTableName().length();

        int startPosition = serializationBuffer.position();

        int headerLength = BINLOG_INFO_OFFSET + binlogLen + EXTRA_FLAGS_LENGTH_BYTES + extraFlagsLen;
        int valueLength = TABLE_NAME_LENGTH_BYTES + tableNameLen + KEY_LENGTH_BYTES + LONG_KEY_BYTES + payloadLen;
        int eventLength = headerLength + valueLength;

        serializationBuffer
            .put(VERSION)
            .putInt(CRC_DEFAULT) // header crc
            // header
            .putInt(eventLength)
            .put(changeEntry.getSourceType().getSourceTypeCode())
            
            .put(changeEntry.getEventType().getCode())
            .putLong(changeEntry.getFromServerIp())
            .put(EventKey.KeyType.LONG.getCode())
            .putShort(key.getLogicalPartitionId())
            .putLong(changeEntry.getTimestamp())
            .putInt(CRC_DEFAULT) // body crc
            .put((byte) changeEntry.getBinlogInfo().getLength())
            .put(changeEntry.getBinlogInfo().encode())
            .putInt(extraFlagsLen)
            .put(extraFlagsBytes)
            // body
            .putShort((short) tableNameLen)
            .put(SerializeUtil.getBytes(changeEntry.getTableName()))
            .putInt(LONG_KEY_BYTES)
            .putLong(key.getLongKey())
            .put(valueBytes);

        int stopPosition = serializationBuffer.position();

        long valueCrc = CRCUtil.getChecksum(serializationBuffer, startPosition + headerLength, valueLength);
        BufferUtil.putUnsignedInt(serializationBuffer, startPosition + BODY_CRC_OFFSET, valueCrc);
        long headerCrc = CRCUtil.getChecksum(serializationBuffer, startPosition + EVENT_LENGTH_OFFSET, headerLength - EVENT_LENGTH_OFFSET);
        BufferUtil.putUnsignedInt(serializationBuffer, startPosition + HEADER_CRC_OFFSET, headerCrc);

        serializationBuffer.position(stopPosition);
        return (stopPosition - startPosition);
    }

    private static int serializeStringKeyEvent(EventKey key, ChangeEntry changeEntry, ByteBuffer serializationBuffer) {
        byte[] valueBytes = changeEntry.getSerializedRecord();
        byte[] extraFlagsBytes = changeEntry.getSerializedExtraFlags();
        byte[] keyBytes = key.getStringKeyInBytes();

        int payloadLen = valueBytes.length;
        int extraFlagsLen = extraFlagsBytes.length;
        int binlogLen = changeEntry.getBinlogInfo().getLength();
        int tableNameLen = changeEntry.getTableName().length();
        int keyLen = keyBytes.length;

        int startPosition = serializationBuffer.position();

        int headerLength = BINLOG_INFO_OFFSET + binlogLen + EXTRA_FLAGS_LENGTH_BYTES + extraFlagsLen;
        int valueLength = TABLE_NAME_LENGTH_BYTES + tableNameLen + KEY_LENGTH_BYTES + keyLen + payloadLen;
        int eventLength = headerLength + valueLength;

        serializationBuffer
            .put(VERSION)
            .putInt(CRC_DEFAULT) // header crc
            // header
            .putInt(eventLength)
            .put(changeEntry.getSourceType().getSourceTypeCode())
            
            .put(changeEntry.getEventType().getCode())
            .putLong(changeEntry.getFromServerIp())
            .put(EventKey.KeyType.STRING.getCode())
            .putShort(key.getLogicalPartitionId())
            .putLong(changeEntry.getTimestamp())
            .putInt(CRC_DEFAULT) // body crc
            .put((byte) changeEntry.getBinlogInfo().getLength())
            .put(changeEntry.getBinlogInfo().encode())
            .putInt(extraFlagsLen)
            .put(extraFlagsBytes)
            // body
            .putShort((short) tableNameLen)
            .put(SerializeUtil.getBytes(changeEntry.getTableName()))
            .putInt(keyLen)
            .put(keyBytes)
            .put(valueBytes);

        int stopPosition = serializationBuffer.position();

        long valueCrc = CRCUtil.getChecksum(serializationBuffer, startPosition + headerLength, valueLength);
        BufferUtil.putUnsignedInt(serializationBuffer, startPosition + BODY_CRC_OFFSET, valueCrc);
        long headerCrc = CRCUtil.getChecksum(serializationBuffer, startPosition + EVENT_LENGTH_OFFSET, headerLength - EVENT_LENGTH_OFFSET);
        BufferUtil.putUnsignedInt(serializationBuffer, startPosition + HEADER_CRC_OFFSET, headerCrc);

        serializationBuffer.position(stopPosition);
        return (stopPosition - startPosition);
    }

    public PtubesEventV2() {
        inited = false;
    }

    public PtubesEventV2(ByteBuffer buf, int position) {
        resetInternal(buf, position);
    }

    private byte getSourceTypeCode() {
        return buf.get(position + SOURCE_TYPE_OFFSET);
    }

    private SourceType getSourceType() {
        byte sourceTypeCode = getSourceTypeCode();
        return SourceType.valueOf(sourceTypeCode);
    }

    private int getEventLength() {
        return buf.getInt(position + EVENT_LENGTH_OFFSET);
    }

    private int getBinlogInfoOffset() {
        return BINLOG_INFO_OFFSET;
    }

    private int getBinlogInfoLength() {
        return buf.get(position + BINLOG_INFO_LENGTH_OFFSET);
    }

    private int getExtraFlagsLengthOffset() {
        return getBinlogInfoOffset() + getBinlogInfoLength();
    }

    private int getExtraFlagsLength() {
        return buf.getInt(position + getExtraFlagsLengthOffset());
    }

    private int getExtraFlagsOffset() {
        return getExtraFlagsLengthOffset() + EXTRA_FLAGS_LENGTH_BYTES;
    }

    private int getTableNameLengthOffset() {
        return getExtraFlagsOffset() + getExtraFlagsLength();
    }

    private int getTableNameLength() {
        return buf.getShort(position + getTableNameLengthOffset());
    }

    private int getTableNameOffset() {
        return getTableNameLengthOffset() + TABLE_NAME_LENGTH_BYTES;
    }

    private int getKeyLengthOffset() {
        return getTableNameOffset() + getTableNameLength();
    }

    private int getKeyLength() {
        return buf.getInt(position + getKeyLengthOffset());
    }

    private int getKeyOffset() {
        return getKeyLengthOffset() + KEY_LENGTH_BYTES;
    }

    private int getValueOffset() {
        return getKeyOffset() + getKeyLength();
    }

    private int getValueLength() {
        return getEventLength() - getValueOffset();
    }

    private byte getKeyType() {
        return buf.get(position + KEY_TYPE_OFFSET);
    }

    @Override
    public byte getVersion() {
        return buf.get(position + VERSION_OFFSET);
    }

    @Override
    public EventInternalReadable reset(ByteBuffer buf, int position) {
        byte eventVersion = buf.get(position + VERSION_OFFSET);
        if (eventVersion != VERSION) {
            throw new PtubesRunTimeException("Unexpected event version, expected: " + VERSION + ", actual: " + eventVersion);
        }

        inited = true;
        this.buf = buf;
        this.position = position;
        return this;
    }

    @Override public long getHeaderCrc() {
        return BufferUtil.getUnsignedInt(buf, position + HEADER_CRC_OFFSET);
    }

    @Override public EventScanStatus scanEvent(boolean logErrors) {
        HeaderScanStatus headerScanStatus = scanHeader(logErrors);

        if (headerScanStatus != HeaderScanStatus.OK) {
            if (logErrors) {
                LOG.error("HeaderScan error=" + headerScanStatus);
            }
            return (headerScanStatus == HeaderScanStatus.ERR ? EventScanStatus.ERR : EventScanStatus.PARTIAL);
        }

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

    @Override public int getPayloadLength() {
        return size() - getTableNameLengthOffset();
    }

    @Override public long getValueCrc() {
        return BufferUtil.getUnsignedInt(buf, position + BODY_CRC_OFFSET);
    }

    @Override public long getCalculatedValueCrc() {
        long calcValueCrc = CRCUtil.getChecksum(buf, position + getTableNameLengthOffset(), getPayloadLength());
        return calcValueCrc;
    }

    @Override public ByteBuffer getRawBytes() {
        ByteBuffer buffer = buf.asReadOnlyBuffer().order(buf.order());
        buffer.position(position);
        buffer.limit(position + size());
        return buffer;
    }

    // why: Is there any problem with using view buffer directly?
    @Override public EventInternalWritable createCopy() {
        ByteBuffer cloned = ByteBuffer.allocate(size()).order(buf.order());
        cloned.put(getRawBytes());
        PtubesEventV2 c = new PtubesEventV2(cloned, 0);
        return c;
    }

    @Override protected HeaderScanStatus scanHeader(boolean logErrors) {
        if (getVersion() != VERSION) {
            if (logErrors) {
                LOG.error("unknown version byte in header: " + getVersion());
            }
            return HeaderScanStatus.ERR;
        }

        int bytesInBuffer = buf.limit() - position;
        int expectedLength = FIXED_HEADER_BYTES;
        if (bytesInBuffer < expectedLength) {
            return HeaderScanStatus.PARTIAL;
        }
        expectedLength = expectedLength + getBinlogInfoLength() + EXTRA_FLAGS_LENGTH_BYTES;
        if (bytesInBuffer < expectedLength) {
            return HeaderScanStatus.PARTIAL;
        }
        expectedLength = expectedLength + getExtraFlagsLength();
        if (bytesInBuffer < expectedLength) {
            return HeaderScanStatus.PARTIAL;
        }

        // check header crc
        long calculatedHeaderCrc = CRCUtil.getChecksum(buf, position + EVENT_LENGTH_OFFSET, expectedLength - EVENT_LENGTH_OFFSET);
        if (calculatedHeaderCrc != getHeaderCrc()) {
            if (logErrors) {
                LOG.error("Header CRC mismatch, getHeaderCrc(): {}, calculatedCrc: {}, expectedLength: {}", getHeaderCrc(), calculatedHeaderCrc, expectedLength);
            }
            return HeaderScanStatus.ERR;
        }
        return HeaderScanStatus.OK;
    }

    @Override protected boolean isPartial() {
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

    @Override public boolean isKeyNumber() {
        return EventKey.KeyType.LONG.getCode() == getKeyType();
    }

    @Override public boolean isKeyString() {
        return EventKey.KeyType.STRING.getCode() == getKeyType();
    }

    @Override public EventType getEventType() {
        return EventType.getByCode(buf.get(position + EVENT_TYPE_OFFSET));
    }

    @Override public String getFromIP() {
        long ipv4Number = buf.getLong(position + FROM_IP_OFFSET);
        return IPUtil.longToIp(ipv4Number, ByteOrder.BIG_ENDIAN.equals(buf.order()));
    }

    @Override public long getTimestampInNS() {
        return buf.getLong(position + TIMESTAMP_OFFSET);
    }

    @Override public int size() {
        return buf.getInt(position + EVENT_LENGTH_OFFSET);
    }

    @Override public byte[] getBinlogInfoByte() {
        int binlogInfoLength = getBinlogInfoLength();
        int binlogInfoOffset = getBinlogInfoOffset();
        byte[] binlogInfo = new byte[binlogInfoLength];
        for (int i = 0; i < binlogInfoLength; i++) {
            binlogInfo[i] = buf.get(position + binlogInfoOffset + i);
        }
        return binlogInfo;
    }

    
    @Override public BinlogInfo getBinlogInfo() {
        byte[] binlogInfoBytes = getBinlogInfoByte();
        byte sourceTypeCode = getSourceTypeCode();
        SourceType sourceType = SourceType.valueOf(sourceTypeCode);
        switch (sourceType) {
            case MySQL:
                return BinlogInfoFactory.decode(SourceType.MySQL, binlogInfoBytes);
            default:
                throw new UnsupportedOperationException("Unknown source type code: " + sourceType.getSourceTypeCode());
        }
    }

    @Override public String getTableName() {
        int tableNameLength = getTableNameLength();
        byte[] tableName = new byte[tableNameLength];
        int start = position + getTableNameLengthOffset() + TABLE_NAME_LENGTH_BYTES;
        for (int i = 0; i < tableNameLength; i++) {
            tableName[i] = buf.get(start + i);
        }
        return SerializeUtil.getString(tableName);
    }

    @Override public long getKey() {
        assert isKeyNumber();
        return buf.getLong(position + getKeyOffset());
    }

    @Override public byte[] getKeyBytes() {
        assert isKeyString();

        int keyLength = getKeyLength();
        int keyOffset = getKeyOffset();
        byte[] dst = new byte[keyLength];

        int start = position + keyOffset;
        int end = start + keyLength;
        ByteBuffer dupBuf = buf.duplicate().order(buf.order());
        dupBuf.position(start);
        dupBuf.limit(end);
        dupBuf.get(dst, 0, keyLength);

        return dst;
    }

    @Override public short getPartitionId() {
        return buf.getShort(position + LOGICAL_PARTITION_ID_OFFSET);
    }

    @Override public byte[] getPayload() {
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

    @Override public EventPart getKeyPart() {
        throw new PtubesRunTimeException("V2 event does not support schema keys");
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
            LOG.error("PtubesEventV2.toString() : Got Exception while trying to validate the event ", ex);
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
        sb.append("[Position=").append(position).append(";Version=").append(getVersion()).append(";eventType=").append(
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
            .append(getValueCrc()).append("]");
        return sb.toString();
    }
}
