package com.meituan.ptubes.reader.container.common.vo;


public class ChangeIdInfo {
	private short changeId;
	private long serverId;
	private String previousGtidSet;
	private int beginBinlogId;
	private long beginBinlogOffset;
	private long beginTimeStampMS;

	public ChangeIdInfo() {
	}

	public ChangeIdInfo(short changeId, long serverId, String previousGtidSet, int beginBinlogId,
			long beginBinlogOffset, long beginTimeStampMS) {
		this.changeId = changeId;
		this.serverId = serverId;
		this.previousGtidSet = previousGtidSet;
		this.beginBinlogId = beginBinlogId;
		this.beginBinlogOffset = beginBinlogOffset;
		this.beginTimeStampMS = beginTimeStampMS;
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

	public String getPreviousGtidSet() {
		return previousGtidSet;
	}

	public void setPreviousGtidSet(String previousGtidSet) {
		this.previousGtidSet = previousGtidSet;
	}

	public int getBeginBinlogId() {
		return beginBinlogId;
	}

	public void setBeginBinlogId(int beginBinlogId) {
		this.beginBinlogId = beginBinlogId;
	}

	public long getBeginBinlogOffset() {
		return beginBinlogOffset;
	}

	public void setBeginBinlogOffset(long beginBinlogOffset) {
		this.beginBinlogOffset = beginBinlogOffset;
	}

	public long getBeginTimeStampMS() {
		return beginTimeStampMS;
	}

	public void setBeginTimeStampMS(long beginTimeStampMS) {
		this.beginTimeStampMS = beginTimeStampMS;
	}

	@Override
	public String toString() {
		return "changeId=" + changeId + ";serverId=" + serverId + ";previousGtidSet="
				+ previousGtidSet + ";beginBinlogId=" + beginBinlogId + ";beginBinlogOffset="
				+ beginBinlogOffset + ";beginTimeStampMS=" + beginTimeStampMS;
	}
}
