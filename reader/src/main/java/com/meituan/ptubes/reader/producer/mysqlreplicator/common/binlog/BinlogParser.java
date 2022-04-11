package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog;

import java.util.List;
import java.util.concurrent.TimeUnit;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.filter.BinlogEventFilter;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.impl.AbstractBinlogParser;

public interface BinlogParser {

	boolean isRunning();

	void start() throws Exception;

	void stop(long timeout, TimeUnit unit) throws Exception;

	void setEventFilter(BinlogEventFilter filter);

	void setEventListener(BinlogEventListener listener);

	List<BinlogParserListener> getParserListeners();

	boolean addParserListener(BinlogParserListener listener);

	boolean removeParserListener(BinlogParserListener listener);

	void setParserListeners(List<BinlogParserListener> listeners);

	AbstractBinlogParser.Context getContext();

	void setContext(AbstractBinlogParser.Context context);
}
