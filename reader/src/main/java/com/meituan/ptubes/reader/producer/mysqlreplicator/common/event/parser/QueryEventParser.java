
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogEventV4Header;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogParserContext;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.StatusVariable;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.QueryEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QAutoIncrement;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QCatalogCode;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QCatalogNzCode;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QCharsetCode;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QCharsetDatabaseCode;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QFlags2Code;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QInvoker;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QLcTimeNamesCode;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QMasterDataWrittenCode;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QMicroseconds;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QSQLModeCode;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QTableMapForUpdateCode;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QTimeZoneCode;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.status.QUpdatedDBNames;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueryEventParser extends AbstractBinlogEventParser {
	public QueryEventParser() {
		super(QueryEvent.EVENT_TYPE);
	}

	@Override
	public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context) throws IOException {
		final QueryEvent event = new QueryEvent(header);
		event.setBinlogFilename(context.getBinlogFileName());
		event.setThreadId(is.readLong(4));
		event.setElapsedTime(is.readLong(4));
		event.setDatabaseNameLength(is.readInt(1));
		event.setErrorCode(is.readInt(2));
		event.setStatusVariablesLength(is.readInt(2));
		event.setRawStatusVariables(is.readBytes(event.getStatusVariablesLength()));
		event.setDatabaseName(is.readNullTerminatedString());
		event.setSql(is.readFixedLengthString(is.available()));
		context.getEventListener().onEvents(event);
	}

	public static List<StatusVariable> parseStatusVariables(byte[] data) throws IOException {
		final List<StatusVariable> r = new ArrayList<StatusVariable>();
		final XDeserializer d = new XDeserializer(data);
		boolean abort = false;
		while (!abort && d.available() > 0) {
			final int type = d.readInt(1);
			switch (type) {
			case QAutoIncrement.TYPE:
				r.add(QAutoIncrement.valueOf(d));
				break;
			case QCatalogCode.TYPE:
				r.add(QCatalogCode.valueOf(d));
				break;
			case QCatalogNzCode.TYPE:
				r.add(QCatalogNzCode.valueOf(d));
				break;
			case QCharsetCode.TYPE:
				r.add(QCharsetCode.valueOf(d));
				break;
			case QCharsetDatabaseCode.TYPE:
				r.add(QCharsetDatabaseCode.valueOf(d));
				break;
			case QFlags2Code.TYPE:
				r.add(QFlags2Code.valueOf(d));
				break;
			case QLcTimeNamesCode.TYPE:
				r.add(QLcTimeNamesCode.valueOf(d));
				break;
			case QSQLModeCode.TYPE:
				r.add(QSQLModeCode.valueOf(d));
				break;
			case QTableMapForUpdateCode.TYPE:
				r.add(QTableMapForUpdateCode.valueOf(d));
				break;
			case QTimeZoneCode.TYPE:
				r.add(QTimeZoneCode.valueOf(d));
				break;
			case QMasterDataWrittenCode.TYPE:
				r.add(QMasterDataWrittenCode.valueOf(d));
				break;
			case QInvoker.TYPE:
				r.add(QInvoker.valueOf(d));
				break;
			case QUpdatedDBNames.TYPE:
				r.add(QUpdatedDBNames.valueOf(d));
				break;
			case QMicroseconds.TYPE:
				r.add(QMicroseconds.valueOf(d));
				break;
			default:
				LOG.warn("unknown status variable type: " + type);
				abort = true;
				break;
			}
		}
		return r;
	}
}
