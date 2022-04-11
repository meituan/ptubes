package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.filter;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;

public interface BinlogEventFilter {

	boolean accepts(BinlogEventV4Header header, BinlogParserContext context);
}
