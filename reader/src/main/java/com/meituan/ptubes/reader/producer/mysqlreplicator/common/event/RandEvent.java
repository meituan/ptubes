package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

/**
 * Written every time a statement uses the RAND() function; precedes other events for the statement.
 * Indicates the seed values to use for generating a random number with RAND() in the next statement.
 * This is written only before a QUERY_EVENT and is not used with row-based logging.
 */
public final class RandEvent extends AbstractBinlogEventV4 {
	public static final int EVENT_TYPE = MySQLConstants.RAND_EVENT;

	private long randSeed1;
	private long randSeed2;

	public RandEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("randSeed1", randSeed1).append("randSeed2",
				randSeed2).toString();
	}

	public long getRandSeed1() {
		return randSeed1;
	}

	public void setRandSeed1(long randSeed1) {
		this.randSeed1 = randSeed1;
	}

	public long getRandSeed2() {
		return randSeed2;
	}

	public void setRandSeed2(long randSeed2) {
		this.randSeed2 = randSeed2;
	}
}
