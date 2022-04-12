package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.filter;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogRowEventFilter;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public class BinlogRowEventFilterImpl implements BinlogRowEventFilter {

	private static final Logger LOG = LoggerFactory.getLogger(BinlogRowEventFilterImpl.class);

	private boolean verbose = true;

	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override public boolean accepts(BinlogEventV4Header header, BinlogParserContext context, TableMapEvent event, long tableId) {
		if (event == null) {
			if (isVerbose() && LOG.isWarnEnabled()) {
				LOG.warn("failed to find TableMapEvent, header: " + header + " tableId:" + tableId);
			}
			return false;
		}

		return true;
	}
}
