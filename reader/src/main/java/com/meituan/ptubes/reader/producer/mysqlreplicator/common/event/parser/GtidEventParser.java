package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.GtidEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;

/**
 * GTID Event
 *
 * Event format:
 * +-------------------+
 * | 1B commit flag    |
 * +-------------------+
 * | 16B Source ID     |
 * +-------------------+
 * | 8B Txn ID         |
 * +-------------------+
 * | ...               |
 * +-------------------+
 *
 */
public class GtidEventParser extends AbstractBinlogEventParser {

	public GtidEventParser() {
		super(MySQLConstants.GTID_LOG_EVENT);
	}

	@Override
	public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		GtidEvent event = new GtidEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		// commit flag, always true
		int flags = is.readInt(1);
		byte[] uuid = is.readBytes(16);
		event.setFlag(flags);
		event.setTransactionId(is.readLong(8, true));
		event.setUuid(uuid);

		// position at next event
		skip(is, is.available());
		context.getEventListener().onEvents(event);
	}

	private String byteArrayToHex(byte[] a, int offset, int len) {
		StringBuilder sb = new StringBuilder();
		for (int idx = offset; idx < (offset + len) && idx < a.length; idx++) {
			sb.append(String.format("%02x", a[idx] & 0xff));
		}
		return sb.toString();
	}
}
