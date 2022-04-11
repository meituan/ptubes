package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog;

import java.io.IOException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

public interface BinlogEventParser {

	Logger LOG = LoggerFactory.getLogger(BinlogEventParser.class);

	int getEventType();

	void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException;

	void skip(XInputStream is, int skipLen) throws IOException;
}
