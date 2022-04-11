package com.meituan.ptubes.reader.container.common.vo;

import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessageV3;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import com.meituan.ptubes.sdk.protocol.RdsPacket;


public class MySQLBinlogInfo implements BinlogInfo {

	public static final int LENGTH = 58;

	// total 56bytes
	private volatile short changeId = -1;
	// unsinged int，4byte
	private volatile long serverId = 0L;
	private volatile int binlogId = -1;
	private volatile long binlogOffset = -1L;
	//uuid in gtid, 16Byte
	private volatile byte[] uuid = new byte[16];
	private volatile long txnId = -1L;
	private volatile long eventIndex = -1L;
	private volatile long timestamp = -1L;
	private volatile long incrementalLabel = -1L;

	public static int getSizeInByte() {
		return LENGTH;
	}

	@Override
	public int getLength() {
		return LENGTH;
	}

	public short getChangeId() {
		return changeId;
	}

	public void setChangeId(short changeId) {
		this.changeId = changeId;
	}

	public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	public int getBinlogId() {
		return binlogId;
	}

	public void setBinlogId(int binlogId) {
		this.binlogId = binlogId;
	}

	public long getBinlogOffset() {
		return binlogOffset;
	}

	public void setBinlogOffset(long binlogOffset) {
		this.binlogOffset = binlogOffset;
	}

	public byte[] getUuid() {
		return uuid;
	}

	public void setUuid(byte[] uuid) {
		this.uuid = uuid;
	}

	public long getTxnId() {
		return txnId;
	}

	public void setTxnId(long txnId) {
		this.txnId = txnId;
	}

	public long getEventIndex() {
		return eventIndex;
	}

	public void setEventIndex(long eventIndex) {
		this.eventIndex = eventIndex;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override public long getIncrementalLabel() {
		return incrementalLabel;
	}

	public void setIncrementalLabel(long incrementalLabel) {
		this.incrementalLabel = incrementalLabel;
	}

	public MySQLBinlogInfo() {
	}

	public MySQLBinlogInfo(short changeId, long serverId, int binlogId, long binlogOffset, byte[] uuid, long txnId,
			long eventIndex, long timestamp) {
		this.changeId = changeId;
		this.serverId = serverId;
		this.binlogId = binlogId;
		this.binlogOffset = binlogOffset;
		this.uuid = uuid;
		this.txnId = txnId;
		this.eventIndex = eventIndex;
		this.timestamp = timestamp;
	}

	public MySQLBinlogInfo(String binlogInfoStr) {
		String[] binlogInfoStrs = binlogInfoStr.split(",");
		for (String str : binlogInfoStrs) {
			String[] kvs = str.split("=");

		}
		
	}

	@Override
	public void decode(byte[] data) {
		ByteBuf buf = Unpooled.wrappedBuffer(data);
		this.changeId = buf.readShort();
		this.serverId = Long.valueOf(buf.readInt());
		this.binlogId = buf.readInt();
		this.binlogOffset = buf.readLong();
		byte[] uuid = new byte[16];
		buf.readBytes(uuid, 0, 16);
		this.uuid = uuid;
		this.txnId = buf.readLong();
		this.eventIndex = buf.readLong();
		this.timestamp = buf.readLong();
	}

	@Override
	public byte[] encode() {
		ByteBuffer buf = ByteBuffer.allocate(getSizeInByte());
		buf.putShort(changeId);
		buf.putInt((int) serverId);
		buf.putInt(binlogId);
		buf.putLong(binlogOffset);
		buf.put(uuid);
		buf.putLong(txnId);
		buf.putLong(eventIndex);
		buf.putLong(timestamp);

		return buf.array();
	}

	@Override
	public boolean isGreaterThan(BinlogInfo inputBinlogInfo, StorageConstant.IndexPolicy indexPolicy) {
		Preconditions.checkArgument(inputBinlogInfo instanceof MySQLBinlogInfo, "imcompatible binlogInfo: this=" + this.getClass() + ", input=" + inputBinlogInfo.getClass());

		MySQLBinlogInfo binlogInfo = (MySQLBinlogInfo) inputBinlogInfo;

		if (!binlogInfo.isValid()) {
			return false;
		}
		if (!isValid()) {
			return false;
		}
		if (changeId != binlogInfo.getChangeId()) {
			return changeId > binlogInfo.getChangeId();
		}
		long aServerId = serverId;
		long bServerId = binlogInfo.getServerId();
		switch (indexPolicy) {
		case BINLOG_OFFSET:
			if (aServerId > 0 && bServerId > 0) {
				//serverId is valid, compare serverId
				if (aServerId == bServerId) {
					//The serverId is the same, compare the binlogId
					if (binlogId > 0 && binlogInfo.getBinlogId() > 0) {
						//binlogId is valid, compare binlogId
						if (binlogId == binlogInfo.getBinlogId()) {
							//binlogId is equal, compare binlogOffset
							if (binlogOffset == binlogInfo.getBinlogOffset()) {
								//binlogId is equal, compare binlogOffset
								if (eventIndex == binlogInfo.getEventIndex()) {
									return isGreaterThan(binlogInfo, StorageConstant.IndexPolicy.TIME);
								} else {
									return eventIndex > binlogInfo.getEventIndex();
								}
							} else {
								//Compare binlogOffset
								return binlogOffset > binlogInfo.getBinlogOffset();
							}
						} else {
							//Compare binlogId
							return binlogId > binlogInfo.getBinlogId();
						}
					} else {
						//Compare binlogId
						return isGreaterThan(binlogInfo, StorageConstant.IndexPolicy.TIME);
					}
				} else {
					//The serverId is not the same, compare the time
					return isGreaterThan(binlogInfo, StorageConstant.IndexPolicy.TIME);
				}
			} else {
				//serverId is invalid, compare time
				return isGreaterThan(binlogInfo, StorageConstant.IndexPolicy.TIME);
			}
		case TIME:
			if (timestamp <= 0 || binlogInfo.getTimestamp() <= 0) {
				return false;
			} else {
				if (timestamp == binlogInfo.getTimestamp()) {
					return eventIndex > binlogInfo.getEventIndex();
				} else {
					return timestamp > binlogInfo.getTimestamp();
				}
			}
		default:
			return isGreaterThan(binlogInfo, StorageConstant.IndexPolicy.BINLOG_OFFSET);
		}
	}

	@Override
	public boolean isEqualTo(BinlogInfo inputBinlogInfo, StorageConstant.IndexPolicy indexPolicy) {
		Preconditions.checkArgument(inputBinlogInfo instanceof MySQLBinlogInfo, "imcompatible binlogInfo: this=" + this.getClass() + ", input=" + inputBinlogInfo.getClass());

		MySQLBinlogInfo binlogInfo = (MySQLBinlogInfo) inputBinlogInfo;

		switch (indexPolicy) {
		case BINLOG_OFFSET:
			if (changeId != binlogInfo.getChangeId()) {
				return false;
			}
			if (serverId > 0 && binlogInfo.getServerId() > 0) {
				//serverId is valid, compare serverId
				if (serverId == binlogInfo.getServerId()) {
					//serverId is the same, compare binlogId
					if (binlogId > 0 && binlogInfo.getBinlogId() > 0) {
						//binlogId is valid, compare binlogId
						if (binlogId == binlogInfo.getBinlogId()) {
							//binlogId is equal, compare binlogOffset
							if (binlogOffset == binlogInfo.getBinlogOffset()) {
								//binlogOffset is equal, compare eventIndex
								if (eventIndex == binlogInfo.getEventIndex()) {
									return true;
								} else {
									return false;
								}
							} else {
								//binlogOffset is different
								return false;
							}
						} else {
							//binlogId is different
							return false;
						}
					} else {
						// binlogId is invalid, compare time
						return isEqualTo(binlogInfo, StorageConstant.IndexPolicy.TIME);
					}
				} else {
					//serverId is not the same, compare time
					return isEqualTo(binlogInfo, StorageConstant.IndexPolicy.TIME);
				}
			} else {
				//serverId is invalid, compare time
				return isEqualTo(binlogInfo, StorageConstant.IndexPolicy.TIME);
			}
		case TIME:
			if (timestamp <= 0 || binlogInfo.getTimestamp() <= 0) {
				return true;
			} else {
				return timestamp == binlogInfo.getTimestamp() && eventIndex == binlogInfo.getEventIndex();
			}
		default:
			return isEqualTo(binlogInfo, StorageConstant.IndexPolicy.BINLOG_OFFSET);
		}
	}

	@Override
	public boolean isGreaterEqualThan(BinlogInfo binlogInfo, StorageConstant.IndexPolicy indexPolicy) {
		if (isGreaterThan(binlogInfo, indexPolicy)) {
			return true;
		}
		if (isEqualTo(binlogInfo, indexPolicy)) {
			return true;
		}
		return false;
	}

	@Override
	public boolean isValid() {
		return (binlogId > 0 && binlogOffset > 0) || timestamp > 0;
	}

	@Override
	public String toString() {
		return "changeId=" + changeId + ", serverId=" + serverId + ", binlogId=" + binlogId + ", binlogOffset="
				+ binlogOffset + ", uuid=" + Gtid.getUuidStr(uuid) + ", txnId=" + txnId + ", eventIndex=" + eventIndex
				+ ", timestamp=" + timestamp;
	}

	@Override
	public Map<String, Object> toMap() {
		Map<String, Object> ans = new HashMap<>();

		ans.put("changeId", changeId);
		ans.put("serverId", serverId);
		ans.put("binlogId", binlogId);
		ans.put("binlogOffset", binlogOffset);
		ans.put("uuid", Gtid.getUuidStr(uuid));
		ans.put("txnId", txnId);
		ans.put("eventIndex", eventIndex);
		ans.put("timestamp", timestamp);

		return ans;
	}

	@Override
	public GeneratedMessageV3 toCheckpoint() {
		RdsPacket.Checkpoint.Builder cpktBuilder = RdsPacket.Checkpoint.newBuilder();
		cpktBuilder.setUuid(Gtid.getUuidStr(uuid)).setTransactionId(txnId).setEventIndex(eventIndex)
			.setServerId((int)serverId).setBinlogFile(binlogId).setBinlogOffset(binlogOffset)
			.setTimestamp(timestamp);
		return cpktBuilder.build();
	}

	@Override
	public long getTs() {
		return this.timestamp;
	}

	@Override
	public SourceType getSourceType() {
		return SourceType.MySQL;
	}

	@Override public BinlogInfoComparison compare(BinlogInfo otherBinlogInfo, StorageConstant.IndexPolicy indexPolicy) {
		if (this.isGreaterThan(otherBinlogInfo, indexPolicy)) {
			return BinlogInfoComparison.gt;
		} else if (this.isEqualTo(otherBinlogInfo, indexPolicy)) {
			return BinlogInfoComparison.eq;
		} else {
			return BinlogInfoComparison.lt;
		}
	}
}
