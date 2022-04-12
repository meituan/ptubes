package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.StatusVariable;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser.QueryEventParser;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class QueryEvent extends AbstractBinlogEventV4 {
	public static final int EVENT_TYPE = MySQLConstants.QUERY_EVENT;

	private long threadId;
	private long elapsedTime;
	private int databaseNameLength;
	private int errorCode;
	private int statusVariablesLength;
	private byte[] rawStatusVariables;
	private List<StatusVariable> statusVariables;
	private StringColumn databaseName;
	private StringColumn sql;

	public QueryEvent() {
	}

	public QueryEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("threadId", threadId).append("elapsedTime",
				elapsedTime).append("databaseNameLength", databaseNameLength).append("errorCode", errorCode).append(
				"statusVariablesLength", statusVariablesLength).append("statusVariables", statusVariables).append(
				"databaseName", databaseName).append("sql", sql).toString();
	}

	public long getThreadId() {
		return threadId;
	}

	public void setThreadId(long threadId) {
		this.threadId = threadId;
	}

	public long getElapsedTime() {
		return elapsedTime;
	}

	public void setElapsedTime(long elapsedTime) {
		this.elapsedTime = elapsedTime;
	}

	public int getDatabaseNameLength() {
		return databaseNameLength;
	}

	public void setDatabaseNameLength(int databaseNameLength) {
		this.databaseNameLength = databaseNameLength;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public int getStatusVariablesLength() {
		return statusVariablesLength;
	}

	public void setStatusVariablesLength(int statusVariableLength) {
		this.statusVariablesLength = statusVariableLength;
	}

	public byte[] getRawStatusVariables() {
		return rawStatusVariables;
	}

	public void setRawStatusVariables(byte[] rawStatusVariables) {
		this.rawStatusVariables = rawStatusVariables;
	}

	public List<StatusVariable> getStatusVariables() throws IOException {
		synchronized (this) {
			if (statusVariables == null) {
				synchronized (this) {
					if (rawStatusVariables != null) {
						statusVariables = QueryEventParser.parseStatusVariables(rawStatusVariables);
					} else {
						// empty list
						statusVariables = new ArrayList<>();
					}
				}
			}
		}
		return statusVariables;
	}

	public void setStatusVariables(List<StatusVariable> statusVariables) {
		this.statusVariables = statusVariables;
	}

	public StringColumn getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(StringColumn databaseName) {
		this.databaseName = databaseName;
	}

	public StringColumn getSql() {
		return sql;
	}

	public void setSql(StringColumn sql) {
		this.sql = sql;
	}
}
