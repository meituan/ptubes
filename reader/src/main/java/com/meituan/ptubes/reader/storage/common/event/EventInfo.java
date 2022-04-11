package com.meituan.ptubes.reader.storage.common.event;

import java.nio.ByteBuffer;
import java.util.Arrays;
import com.meituan.ptubes.common.utils.SerializeUtil;


public class EventInfo {
	private EventType opCode;
	private byte[] binlogInfo;
	private short lPartitionId;
	private String tableName;
	private byte[] payloadSchemaMd5;
	private long timeStampInNS;
	private ByteBuffer payloadBuffer;
	private byte eventSerializationVersion;

	public EventInfo(EventType opCode, byte[] binlogInfo, short lPartitionId, String tableName, byte[] payloadSchemaMd5,
			byte[] payload, long timeStampInNS, byte eventSerializationVersion) {
		this.opCode = opCode;
		this.binlogInfo = binlogInfo;
		this.lPartitionId = lPartitionId;
		this.tableName = tableName;
		this.payloadSchemaMd5 = payloadSchemaMd5;
		this.timeStampInNS = timeStampInNS;
		this.payloadBuffer = null;
		if (payload != null) {
			payloadBuffer = ByteBuffer.wrap(payload);
		}
		this.eventSerializationVersion = eventSerializationVersion;
	}

	/** if opCode value is null - it means use default */
	public EventType getOpCode() {
		return opCode;
	}

	public void setOpCode(EventType opCode) {
		this.opCode = opCode;
	}

	public byte[] getBinlogInfo() {
		return binlogInfo;
	}

	public void setBinlogInfo(byte[] binlogInfo) {
		this.binlogInfo = binlogInfo;
	}

	public short getlPartitionId() {
		return lPartitionId;
	}

	public void setlPartitionId(short lPartitionId) {
		this.lPartitionId = lPartitionId;
	}

	public long getTimeStampInNS() {
		return timeStampInNS;
	}

	public void setTimeStampInNS(long timeStampInNS) {
		this.timeStampInNS = timeStampInNS;
	}

	public byte[] getTableNameBytes() {
		return SerializeUtil.getBytes(tableName);
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public byte[] getSchemaId() {
		return payloadSchemaMd5;
	}

	public void setSchemaId(byte[] schemaId) {
		payloadSchemaMd5 = schemaId;
	}

	/** makes a copy in case of read-only ByteBuffer */
	public byte[] getValueBytes() {
		if (payloadBuffer == null) {
            return null;
        }

		if (payloadBuffer.isReadOnly() || !payloadBuffer.hasArray()) { //allocate new array
			byte[] val = new byte[payloadBuffer.remaining()];
			payloadBuffer.get(val);
			return val;
		}
		// else
		return payloadBuffer.array();

	}

	public int getValueLength() {
		if (payloadBuffer == null) {
            return 0;
        }

		return payloadBuffer.remaining();
	}

	public byte getEventSerializationVersion() {
		return eventSerializationVersion;
	}

	public void setEventSerializationVersion(byte eventSerializationVersion) {
		this.eventSerializationVersion = eventSerializationVersion;
	}

	public void setValueByteBuffer(ByteBuffer bb) {
		if (bb != null) {
			payloadBuffer = bb.asReadOnlyBuffer();
		} else {
			payloadBuffer = null;
		}
	}

	public ByteBuffer getValueByteBuffer() {
		return payloadBuffer;
	}

	@Override
	public String toString() {
		return "EventInfo{" + "opCode=" + opCode + ", binlogInfo=" + Arrays.toString(binlogInfo) + ", lPartitionId="
				+ lPartitionId + ", tableName='" + tableName + '\'' + ", payloadSchemaMd5=" + Arrays.toString(
				payloadSchemaMd5) + ", timeStampInNS=" + timeStampInNS + ", payloadBuffer=" + payloadBuffer
				+ ", eventSerializationVersion=" + eventSerializationVersion + '}';
	}
}
