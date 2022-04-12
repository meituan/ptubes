package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.filter;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;

/**
 * filter events which have smaller binlogOffset than configuration
 */
public class BinlogOffsetFilter implements BinlogEventFilter {
    private static final Logger LOG = LoggerFactory.getLogger(BinlogEventFilter.class);
    private final long offset;
    private boolean allow = false;

    public BinlogOffsetFilter(long offset) {
        this.offset = offset;
    }

    @Override
    public boolean accepts(
        BinlogEventV4Header header,
        BinlogParserContext context
    ) {
        if (allow) {
            return true;
        }
        if (header.getPosition() >= offset) {
            LOG.info("Find target position: " + header.getPosition());
            allow = true;
            return true;
        }
        return false;
    }
}
