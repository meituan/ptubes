package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.UserVariable;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;

public final class UserVarEvent extends AbstractBinlogEventV4 {
	public static final int EVENT_TYPE = MySQLConstants.USER_VAR_EVENT;

	private int varNameLength;
	private StringColumn varName;
	private int isNull;
	private int varType;
	private int varCollation;
	private int varValueLength;
	private UserVariable varValue;

	public UserVarEvent(BinlogEventV4Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("header", header).append("varNameLength", varNameLength).append(
				"varName", varName).append("isNull", isNull).append("varType", varType).append("varCollation",
				varCollation).append("varValueLength", varValueLength).append("varValue", varValue).toString();
	}

	public int getVarNameLength() {
		return varNameLength;
	}

	public void setVarNameLength(int varNameLength) {
		this.varNameLength = varNameLength;
	}

	public StringColumn getVarName() {
		return varName;
	}

	public void setVarName(StringColumn varName) {
		this.varName = varName;
	}

	public int getIsNull() {
		return isNull;
	}

	public void setIsNull(int isNull) {
		this.isNull = isNull;
	}

	public int getVarType() {
		return varType;
	}

	public void setVarType(int variableType) {
		this.varType = variableType;
	}

	public int getVarCollation() {
		return varCollation;
	}

	public void setVarCollation(int varCollation) {
		this.varCollation = varCollation;
	}

	public int getVarValueLength() {
		return varValueLength;
	}

	public void setVarValueLength(int varValueLength) {
		this.varValueLength = varValueLength;
	}

	public UserVariable getVarValue() {
		return varValue;
	}

	public void setVarValue(UserVariable varValue) {
		this.varValue = varValue;
	}
}
