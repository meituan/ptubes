package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.filter;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

/**
 * filter events which have smaller binlogTime than configuration
 */
public class TimeEventFilter implements BinlogEventFilter {
	private static final Logger LOG = LoggerFactory.getLogger(TimeEventFilter.class);
	private final long timeInMS;
	private boolean allow = false;

	public TimeEventFilter(long timeInMS) {
		this.timeInMS = timeInMS;
	}

	@Override
	public boolean accepts(BinlogEventV4Header header, BinlogParserContext context) {
		if (allow) {
			return true;
		}
		if (header.getTimestamp() >= timeInMS) {
			LOG.info("Find target position: {} by time: {}", header.getPosition(), timeInMS);
			allow = true;
			return true;
		}
		return false;
	}
}
