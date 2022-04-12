package com.meituan.ptubes.reader.producer;

import com.meituan.ptubes.reader.container.common.constants.SourceType;
import com.meituan.ptubes.reader.container.common.vo.MaxBinlogInfo;
import com.meituan.ptubes.reader.monitor.collector.ReaderTaskStatMetricsCollector;

public interface EventProducer<MAX_BINLOG_INFO extends MaxBinlogInfo> extends ProducerStorage {
	String getReaderTaskName();

	SourceType getSourceType();

	ReaderTaskStatMetricsCollector getMetricsCollector();

	/**
	 * Pull binlog according to the local maxBinlogInfo configuration
 	 */
	void start();

	/**
	 * Pull binlog according to param maxBinlogInfo
	 *
	 * @param maxBinlogInfo
	 */
	void start(MAX_BINLOG_INFO maxBinlogInfo);

	/**
	 * Pull binlog according to time
	 *
	 * @param timeInMS
	 */
	void start(long timeInMS);

	/**
	 * Pull binlog according to binlogId and binlogOffset
	 * This interface will be deprecated in the future because it's out of standard for other binlog sources
	 *
	 * @param binlogId
	 * @param binlogOffset
	 */
	@Deprecated
	void start(int binlogId, long binlogOffset);

	boolean isRunning();

	boolean isPaused();

	void unpause();

	void pause();

	void shutdown();

	void waitForShutdown() throws InterruptedException, IllegalStateException;

	void waitForShutdown(long timeout) throws InterruptedException, IllegalStateException;
}
