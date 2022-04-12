package com.meituan.ptubes.reader.producer.mysqlreplicator.network;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserListener;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public class BinlogNotifyListener extends BinlogParserListener.Adapter {
	private static final Logger LOG = LoggerFactory.getLogger(BinlogNotifyListener.class);
	@Override
	public void onException(BinlogParser parser, Exception exception) {
		LOG.error("binlog parser error!", exception);
	}

	@Override
	public void onStart(BinlogParser parser) {
	}

	@Override
	public void onStop(BinlogParser parser) {
		LOG.warn("binlog parser stopped");
	}
}
