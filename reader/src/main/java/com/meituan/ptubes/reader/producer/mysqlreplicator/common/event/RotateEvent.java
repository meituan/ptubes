
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class RotateEvent extends AbstractBinlogEventV4 {
	public static final int EVENT_TYPE = MySQLConstants.ROTATE_EVENT;

	private long binlogPosition;
	private StringColumn binlogFileName;

	public RotateEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("binlogPosition", binlogPosition).append(
				"binlogFileName", binlogFileName).toString();
	}

	public long getBinlogPosition() {
		return binlogPosition;
	}

	public void setBinlogPosition(long binlogPosition) {
		this.binlogPosition = binlogPosition;
	}

	public StringColumn getBinlogFileName() {
		return binlogFileName;
	}

	public void setBinlogFileName(StringColumn binlogFileName) {
		this.binlogFileName = binlogFileName;
	}
}
