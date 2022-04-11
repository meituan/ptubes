
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;

public interface BinlogRowEventFilter {

	boolean accepts(BinlogEventV4Header header, BinlogParserContext context, TableMapEvent event, long tableId);
}
