package com.meituan.ptubes.reader.storage.common.event;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.SerializeUtil;
import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.KeyPair;
import com.meituan.ptubes.reader.container.common.vo.MaxBinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.container.common.vo.MySQLMaxBinlogInfo;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import org.apache.commons.collections.CollectionUtils;


@NotThreadSafe
public class MySQLChangeEntry implements ChangeEntry {

	private static final Logger LOG = LoggerFactory.getLogger(MySQLChangeEntry.class);

	
	private final byte version = EventFactory.EVENT_V2;
	private final SourceType sourceType = SourceType.MySQL;

	private final String tableName;
	private final long fromServerIp;
	private final MySQLBinlogInfo binlogInfo;
	private final String gtidSet;
	private final long timestamp;
	private final long receiveTimeNS;
	/**
	 * OpCode of this change entry
	 */
	private final EventType eventType;
	/**
	 * Partition Key(s) corresponding to the entry
	 */
	private final List<KeyPair> partitionKeys;
	/**
	 * Client interaction data
	 */
	private final RdsPacket.RdsEvent pbEvent;

	// runtime cache
	private volatile byte[] serializedExtraFlags = null;
	private volatile byte[] serializedClientEvent = null;
	private volatile byte[] serializedChangeEntry = null;

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public long getFromServerIp() {
		return fromServerIp;
	}

	public byte[] getTableNameBytes() {
		return SerializeUtil.getBytes(tableName);
	}

	@Override
	public MySQLBinlogInfo getBinlogInfo() {
		return binlogInfo;
	}

	@Override
	public MaxBinlogInfo getMaxBinlogInfo() {
		return MySQLMaxBinlogInfo.copyFrom(binlogInfo, gtidSet);
	}

	public String getGtidSet() {
		return gtidSet;
	}

	@Override
	public EventType getEventType() {
		return eventType;
	}

	public List<KeyPair> getPkeys() {
		return partitionKeys;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	public long getReceiveTimeNS() {
		return receiveTimeNS;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((partitionKeys == null) ? 0 : partitionKeys.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		MySQLChangeEntry other = (MySQLChangeEntry) obj;
		if (partitionKeys == null) {
			if (other.getPkeys() != null) {
				return false;
			}
		} else if (!partitionKeys.equals(other.getPkeys())) {
			return false;
		}
		return true;
	}

	public MySQLChangeEntry(String tableName, long fromServerIp, MySQLBinlogInfo binlogInfo, long timestampNanos,
			long receiveTimeNS, String gtidSet, EventType opCode, List<KeyPair> pkeys, RdsPacket.RdsEvent pbEvent) {
		if (!CollectionUtils.isNotEmpty(pkeys)) {
			throw new IllegalArgumentException("Incoming pkeys is empty");
		}
		this.tableName = tableName;
		this.fromServerIp = fromServerIp;
		this.binlogInfo = binlogInfo;
		this.timestamp = timestampNanos;
		this.receiveTimeNS = receiveTimeNS;
		this.gtidSet = gtidSet;
		this.eventType = opCode;
		this.partitionKeys = pkeys;
		this.pbEvent = pbEvent;
	}

	@Override
	public byte getVersion() {
		return version;
	}

	@Override
	public SourceType getSourceType() {
		return sourceType;
	}

	@Override
	public Object genEventKey() throws PtubesException {
		List<KeyPair> pairs = partitionKeys;
		if (null == pairs || pairs.isEmpty()) {
			throw new PtubesException("There do not seem to be any keys");
		}

		if (pairs.size() == 1) {
			Object key = pairs.get(0).getKey();
			return key;
		} else {
			// Treat multiple keys as a separate case to avoid unnecessary casts
			Iterator<KeyPair> li = pairs.iterator();
			StringBuilder compositeKey = new StringBuilder();
			while (li.hasNext()) {
				KeyPair kp = li.next();
				Object key = kp.getKey();
				compositeKey.append(key); // key == null, get "null"
			}
			return compositeKey.toString();
		}
	}

	@GuardedBy("serializedExtraFlags")
	@Override public byte[] getSerializedExtraFlags() {
		if (serializedExtraFlags == null) {
			serializedExtraFlags = new byte[0];
		}
		return new byte[0];
	}

	@GuardedBy("serializedRecord")
	@Override public byte[] getSerializedRecord() {
		if (serializedClientEvent == null) {
			if (pbEvent != null) {
				serializedClientEvent = pbEvent.toByteArray();
			} else {
				serializedClientEvent = new byte[0];
			}
		}
		return serializedClientEvent;
	}

	// file storage use
	@Override
	public byte[] getSerializedChangeEntry() {
		if (serializedChangeEntry != null) {
			return serializedChangeEntry;
		}
		int eventLength = EventFactory.computeEventLength(this);

		ByteBuffer byteBuffer = ByteBuffer.allocate(eventLength);

		int byteCount = 0;
		try {
			byteCount = EventFactory.serializeEvent(this, byteBuffer);
		} catch (PtubesException e) {
			throw new PtubesRunTimeException(e.getMessage());
		}

		if (byteCount != eventLength) {
			throw new PtubesRunTimeException(
					"Actual bytes serialized was: " + byteCount + ", expected bytes is: " + eventLength);
		}
		serializedChangeEntry = byteBuffer.array();
		return serializedChangeEntry;
	}

	@Override
	public String toString() {
		return "DbChangeEntry{" + "\n record=" + tableName + "\n eventType=" + eventType +
			"\n binlogInfo=" + binlogInfo
			+ "\n gtidSet=" + gtidSet + "\n timestamp=" + timestamp + '}';
	}

	@Override
	public long getCommitTs() {
		return this.timestamp;
	}

	@Override
	public long getReceiveNanos() {
		return this.receiveTimeNS;
	}

	@Override
	public boolean canAppend(BinlogInfo lastWrittenBinlogInfo, StorageConstant.IndexPolicy indexPolicy) {
		// Each line of mysql's binlogInfo is unique and needs to satisfy binlogInfo>lastWrittenBinlogInfo to write
		return this.binlogInfo.isGreaterThan(lastWrittenBinlogInfo, indexPolicy);
	}
}
