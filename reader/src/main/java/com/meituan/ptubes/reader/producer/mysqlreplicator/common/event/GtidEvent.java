package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public class GtidEvent extends AbstractBinlogEventV4 {
	private int flag;
	private byte[] uuid;
	private long transactionId;
	private String prevGtidSet;

	public GtidEvent() {
	}

	public GtidEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("transactionId", transactionId).append(
				"uuid", uuid).toString();
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	public byte[] getUuid() {
		return uuid;
	}

	public void setUuid(byte[] uuid) {
		this.uuid = uuid;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(long transactionId) {
		this.transactionId = transactionId;
	}

	public String getPrevGtidSet() {
		return prevGtidSet;
	}

	public void setPrevGtidSet(String prevGtidSet) {
		this.prevGtidSet = prevGtidSet;
	}
}
