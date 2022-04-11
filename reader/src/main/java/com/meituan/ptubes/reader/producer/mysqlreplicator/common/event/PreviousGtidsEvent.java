package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.container.common.vo.GtidSet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;

public class PreviousGtidsEvent extends AbstractBinlogEventV4 {
	private GtidSet previousGtids;

	public PreviousGtidsEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return previousGtids.toString();
	}

	public GtidSet getPreviousGtids() {
		return previousGtids;
	}

	public void setPreviousGtids(GtidSet previousGtids) {
		this.previousGtids = previousGtids;
	}
}
