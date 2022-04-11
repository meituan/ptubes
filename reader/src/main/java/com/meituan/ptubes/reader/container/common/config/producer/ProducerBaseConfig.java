package com.meituan.ptubes.reader.container.common.config.producer;

import com.meituan.ptubes.reader.container.common.constants.SourceType;

public class ProducerBaseConfig {
	// default for mysql
	private SourceType sourceType = SourceType.MySQL;
	private String binlogFilePrefix = "mysql-bin";
	private long maxBinlogInfoFlushIntervalSec = 60;
	private long noEventsConnectionResetTimeSec = 2000 * 1000;
	private long heartIntervalSec = 300;
	private volatile boolean enableStoreSQL = false;

	public ProducerBaseConfig() {
	}

	public SourceType getSourceType() {
		return sourceType;
	}

	public void setSourceType(SourceType sourceType) {
		this.sourceType = sourceType;
	}

	public String getBinlogFilePrefix() {
		return binlogFilePrefix;
	}

	public void setBinlogFilePrefix(String binlogFilePrefix) {
		this.binlogFilePrefix = binlogFilePrefix;
	}

	public long getMaxBinlogInfoFlushIntervalSec() {
		return maxBinlogInfoFlushIntervalSec;
	}

	public void setMaxBinlogInfoFlushIntervalSec(long maxBinlogInfoFlushIntervalSec) {
		this.maxBinlogInfoFlushIntervalSec = maxBinlogInfoFlushIntervalSec;
	}

	public long getNoEventsConnectionResetTimeSec() {
		return noEventsConnectionResetTimeSec;
	}

	public void setNoEventsConnectionResetTimeSec(long noEventsConnectionResetTimeSec) {
		this.noEventsConnectionResetTimeSec = noEventsConnectionResetTimeSec;
	}

	public long getHeartIntervalSec() {
		return heartIntervalSec;
	}

	public void setHeartIntervalSec(long heartIntervalSec) {
		this.heartIntervalSec = heartIntervalSec;
	}

	public boolean isEnableStoreSQL() {
		return enableStoreSQL;
	}

	public void setEnableStoreSQL(boolean enableStoreSQL) {
		this.enableStoreSQL = enableStoreSQL;
	}
}
