package com.meituan.ptubes.reader.container.common.vo;

/**
 * When the disk is scheduled to be placed, the service is restarted or reconnected, the data is restored from maxBinlogInfo
 */
public class MySQLMaxBinlogInfo implements MaxBinlogInfo {

	private volatile int changeId = 0;
	private volatile long serverId = -1L;
	private volatile int binlogId = -1;
	private volatile long binlogOffset = -1L;
	private volatile long eventIndex = -1;
	private volatile Gtid gtid = new Gtid(new byte[16], -1L);
	private volatile String gtidSet = "";
	private volatile long binlogTime;
	private volatile long refreshTime;
	private volatile long incrementalLabel;

	public MySQLMaxBinlogInfo() {
	}

	public MySQLMaxBinlogInfo(int changeId, long serverId) {
		this.changeId = changeId;
		this.serverId = serverId;
		this.refreshTime = System.currentTimeMillis();
		this.binlogTime = System.currentTimeMillis();
	}

	public MySQLMaxBinlogInfo(int changeId, long serverId, int binlogFile, long binlogOffset, long eventIndex, Gtid gtid,
			String gtidSet, long binlogTime, long refreshTime) {
		this.changeId = changeId;
		this.serverId = serverId;
		this.binlogId = binlogFile;
		this.binlogOffset = binlogOffset;
		this.eventIndex = eventIndex;
		this.gtid = gtid;
		this.gtidSet = gtidSet;
		this.binlogTime = binlogTime;
		this.refreshTime = refreshTime;
	}

	public static MySQLMaxBinlogInfo copyFrom(MySQLBinlogInfo binlogInfo, String gtidSetStr) {
		MySQLMaxBinlogInfo mySQLMaxBinlogInfo = new MySQLMaxBinlogInfo();
		mySQLMaxBinlogInfo.changeId = binlogInfo.getChangeId();
		mySQLMaxBinlogInfo.serverId = binlogInfo.getServerId();
		mySQLMaxBinlogInfo.binlogId = binlogInfo.getBinlogId();
		mySQLMaxBinlogInfo.binlogOffset = binlogInfo.getBinlogOffset();
		mySQLMaxBinlogInfo.eventIndex = binlogInfo.getEventIndex();
		mySQLMaxBinlogInfo.binlogTime = binlogInfo.getTimestamp();
		Gtid newGtid = new Gtid(binlogInfo.getUuid(), binlogInfo.getTxnId());
		mySQLMaxBinlogInfo.gtid = new Gtid(newGtid);
		mySQLMaxBinlogInfo.gtidSet = gtidSetStr;
		mySQLMaxBinlogInfo.refreshTime = System.currentTimeMillis();
		return mySQLMaxBinlogInfo;
	}

	@Deprecated
	public void refreshBinlogInfo(MySQLBinlogInfo binlogInfo, String gtidSetStr) {
		this.changeId = binlogInfo.getChangeId();
		this.serverId = binlogInfo.getServerId();
		this.binlogId = binlogInfo.getBinlogId();
		this.binlogOffset = binlogInfo.getBinlogOffset();
		this.eventIndex = binlogInfo.getEventIndex();
		this.binlogTime = binlogInfo.getTimestamp();
		Gtid newGtid = new Gtid(binlogInfo.getUuid(), binlogInfo.getTxnId());
		this.gtid = new Gtid(newGtid);
		this.gtidSet = gtidSetStr;
		this.refreshTime = System.currentTimeMillis();
	}

	public int getChangeId() {
		return changeId;
	}

	public void setChangeId(int changeId) {
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

	public long getEventIndex() {
		return eventIndex;
	}

	public void setEventIndex(long eventIndex) {
		this.eventIndex = eventIndex;
	}

	public Gtid getGtid() {
		return gtid;
	}

	public void setGtid(Gtid gtid) {
		this.gtid = gtid;
	}

	public String getGtidSet() {
		return gtidSet;
	}

	public void setGtidSet(String gtidSet) {
		this.gtidSet = gtidSet;
	}

	public long getBinlogTime() {
		return binlogTime;
	}

	public void setBinlogTime(long binlogTime) {
		this.binlogTime = binlogTime;
	}

	public long getRefreshTime() {
		return refreshTime;
	}

	public void setRefreshTime(long refreshTime) {
		this.refreshTime = refreshTime;
	}

	public long getIncrementalLabel() {
		return incrementalLabel;
	}

	public void setIncrementalLabel(long incrementalLabel) {
		this.incrementalLabel = incrementalLabel;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("changeId=" + changeId)
			.append(SEPERATOR).append("serverId=").append(serverId)
			.append(SEPERATOR).append("binlogId=").append(binlogId)
			.append(SEPERATOR).append("binlogOffset=").append(binlogOffset)
			.append(SEPERATOR).append("eventIndex=").append(eventIndex)
			.append(SEPERATOR).append("gtid=").append(gtid)
			.append(SEPERATOR).append("gtidSet=").append(gtidSet)
			.append(SEPERATOR).append("binlogTime=").append(binlogTime)
			.append(SEPERATOR).append("refreshTime=").append(refreshTime)
			.append(SEPERATOR).append("incrementalLabel=").append(incrementalLabel);
		return sb.toString();
	}
}
