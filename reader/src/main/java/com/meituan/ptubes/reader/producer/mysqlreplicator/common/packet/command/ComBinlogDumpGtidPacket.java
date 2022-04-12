package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet.command;

import java.io.IOException;
import java.util.Collection;
import com.meituan.ptubes.reader.container.common.vo.Gtid;
import com.meituan.ptubes.reader.container.common.vo.GtidSet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XSerializer;

public class ComBinlogDumpGtidPacket extends AbstractCommandPacket {
	private long serverId;
	private int binlogFlag;
	private String binlogFilename;
	private long binlogPosition;
	private GtidSet gtidSet;

	public ComBinlogDumpGtidPacket(long binlogPosition, long serverId, String binlogFileName, GtidSet gtidSet) {
		super(MySQLConstants.COM_BINLOG_DUMP_GTID);
		this.binlogFilename = binlogFileName;
		this.binlogPosition = binlogPosition;
		this.binlogFlag = 0;
		this.serverId = serverId;
		this.gtidSet = gtidSet;
	}

	@Override public byte[] getPacketBody() throws IOException {
		final XSerializer ps = new XSerializer();
		ps.writeInt(this.command, 1);
		ps.writeInt(this.binlogFlag, 2); // flag
		ps.writeLong(this.serverId, 4);
		ps.writeInt(this.binlogFilename.length(), 4);
		ps.writeString(this.binlogFilename);
		ps.writeLong(this.binlogPosition, 8);
		Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
		int dataSize = 8 /* number of uuidSets */;
		for (GtidSet.UUIDSet uuidSet : uuidSets) {
			dataSize += 16 /* uuid */ + 8 /* number of intervals */
					+ uuidSet.getIntervals().size() /* number of intervals */ * 16 /* start-end */;
		}
		ps.writeInt(dataSize, 4);
		ps.writeLong(uuidSets.size(), 8);
		for (GtidSet.UUIDSet uuidSet : uuidSets) {
			ps.writeBytes(Gtid.getUuidByte(uuidSet.getUUID()));
			Collection<GtidSet.Interval> intervals = uuidSet.getIntervals();
			ps.writeLong(intervals.size(), 8);
			for (GtidSet.Interval interval : intervals) {
				ps.writeLong(interval.getStart(), 8);
				ps.writeLong(interval.getEnd() + 1 /* right-open */, 8);
			}
		}
		return ps.toByteArray();
	}

	private static byte[] hexToByteArray(String uuid) {
		byte[] b = new byte[uuid.length() / 2];
		for (int i = 0, j = 0; j < uuid.length(); j += 2) {
			b[i++] = (byte) Integer.parseInt(uuid.charAt(j) + "" + uuid.charAt(j + 1), 16);
		}
		return b;
	}

	public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	public String getBinlogFilename() {
		return binlogFilename;
	}

	public void setBinlogFilename(String binlogFilename) {
		this.binlogFilename = binlogFilename;
	}

	public long getBinlogPosition() {
		return binlogPosition;
	}

	public void setBinlogPosition(long binlogPosition) {
		this.binlogPosition = binlogPosition;
	}

	public GtidSet getGtidSet() {
		return gtidSet;
	}

	public void setGtidSet(GtidSet gtidSet) {
		this.gtidSet = gtidSet;
	}

	public int getBinlogFlag() {
		return binlogFlag;
	}

	public void setBinlogFlag(int binlogFlag) {
		this.binlogFlag = binlogFlag;
	}
}
