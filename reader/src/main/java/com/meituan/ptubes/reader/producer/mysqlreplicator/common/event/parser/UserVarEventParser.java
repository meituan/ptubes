
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.UserVariable;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.UserVarEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.user.UserVariableDecimal;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.user.UserVariableInt;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.user.UserVariableReal;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.user.UserVariableRow;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.user.UserVariableString;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;

import java.io.IOException;

public class UserVarEventParser extends AbstractBinlogEventParser {
	public UserVarEventParser() {
		super(UserVarEvent.EVENT_TYPE);
	}

	@Override public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final UserVarEvent event = new UserVarEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setVarNameLength(is.readInt(4));
		event.setVarName(is.readFixedLengthString(event.getVarNameLength()));
		event.setIsNull(is.readInt(1));
		if (event.getIsNull() == 0) {
			event.setVarType(is.readInt(1));
			event.setVarCollation(is.readInt(4));
			event.setVarValueLength(is.readInt(4));
			event.setVarValue(parseUserVariable(is, event));
		}
		context.getEventListener().onEvents(event);
	}

	protected UserVariable parseUserVariable(XInputStream is, UserVarEvent event) throws IOException {
		final int type = event.getVarType();
		switch (type) {
		case UserVariableDecimal.TYPE:
			return new UserVariableDecimal(is.readBytes(event.getVarValueLength()));
		case UserVariableInt.TYPE:
			return new UserVariableInt(is.readLong(event.getVarValueLength()), is.readInt(1));
		case UserVariableReal.TYPE:
			return new UserVariableReal(Double.longBitsToDouble(is.readLong(event.getVarValueLength())));
		case UserVariableRow.TYPE:
			return new UserVariableRow(is.readBytes(event.getVarValueLength()));
		case UserVariableString.TYPE:
			return new UserVariableString(is.readBytes(event.getVarValueLength()), event.getVarCollation());
		default:
			LOG.warn("unknown user variable type: " + type);
			return null;
		}
	}
}
