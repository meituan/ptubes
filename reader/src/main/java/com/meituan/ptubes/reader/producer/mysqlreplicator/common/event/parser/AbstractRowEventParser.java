
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.parser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog.BinlogRowEventFilter;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.BitColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.BlobColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.DateColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Datetime2Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.DatetimeColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.DecimalColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.DoubleColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.EnumColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.FloatColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.GeometryColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Int24Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.LongColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.LongLongColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Metadata;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.NullColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.SetColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.ShortColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Time2Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.TimeColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Timestamp2Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.TimestampColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.TinyColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.YearColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.TableMapEvent;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.event.filter.BinlogRowEventFilterImpl;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.io.XInputStream;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.CodecUtils;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLConstants;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.MySQLUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;

public abstract class AbstractRowEventParser extends AbstractBinlogEventParser {
	//
	protected BinlogRowEventFilter rowEventFilter;

	public AbstractRowEventParser(int eventType) {
		super(eventType);
		this.rowEventFilter = new BinlogRowEventFilterImpl();
	}

	public BinlogRowEventFilter getRowEventFilter() {
		return rowEventFilter;
	}

	public void setRowEventFilter(BinlogRowEventFilter filter) {
		this.rowEventFilter = filter;
	}

	protected Row parseRow(XInputStream is, TableMapEvent tme, BitColumn usedColumns) throws IOException {
		//
		int unusedColumnCount = 0;
		final byte[] types = tme.getColumnTypes();
		final Metadata metadata = tme.getColumnMetadata();
		final int nColumnsUsed = usedColumns.getSetBitCount();
		final BitColumn nullColumns = is.readBit(nColumnsUsed);
		final List<Column> columns = new ArrayList<Column>(nColumnsUsed);
		for (int i = 0; i < types.length; ++i) {
			//
			int length = 0;
			final int meta = metadata.getMetadata(i);
			int type = CodecUtils.toUnsigned(types[i]);
			if (type == MySQLConstants.TYPE_STRING && meta > 256) {
				final int meta0 = meta >> 8;
				final int meta1 = meta & 0xFF;
				if ((meta0 & 0x30) != 0x30) { // a long CHAR() field: see #37426
					type = meta0 | 0x30;
					length = meta1 | (((meta0 & 0x30) ^ 0x30) << 4);
				} else {
					switch (meta0) {
					case MySQLConstants.TYPE_SET:
					case MySQLConstants.TYPE_ENUM:
					case MySQLConstants.TYPE_STRING:
						type = meta0;
						length = meta1;
						break;
					default:
						throw new RuntimeException("assertion failed, unknown column type: " + type);
					}
				}
			}

			//
			if (!usedColumns.get(i)) {
				unusedColumnCount++;
				continue;
			} else if (nullColumns.get(i - unusedColumnCount)) {
				columns.add(NullColumn.valueOf(type));
				continue;
			}

			//
			switch (type) {
			case MySQLConstants.TYPE_TINY:
				columns.add(TinyColumn.valueOf(is.readSignedInt(1)));
				break;
			case MySQLConstants.TYPE_SHORT:
				columns.add(ShortColumn.valueOf(is.readSignedInt(2)));
				break;
			case MySQLConstants.TYPE_INT24:
				columns.add(Int24Column.valueOf(is.readSignedInt(3)));
				break;
			case MySQLConstants.TYPE_LONG:
				columns.add(LongColumn.valueOf(is.readSignedInt(4)));
				break;
			case MySQLConstants.TYPE_LONGLONG:
				columns.add(LongLongColumn.valueOf(is.readSignedLong(8)));
				break;
			case MySQLConstants.TYPE_FLOAT:
				columns.add(FloatColumn.valueOf(Float.intBitsToFloat(is.readInt(4))));
				break;
			case MySQLConstants.TYPE_DOUBLE:
				columns.add(DoubleColumn.valueOf(Double.longBitsToDouble(is.readLong(8))));
				break;
			case MySQLConstants.TYPE_YEAR:
				columns.add(YearColumn.valueOf(MySQLUtils.toYear(is.readInt(1))));
				break;
			case MySQLConstants.TYPE_DATE:
				Pair<DateTime, String> dateValue = MySQLUtils.toDate(is.readInt(3));
				columns.add(DateColumn.valueOf(dateValue.getLeft(), dateValue.getRight()));
				break;
			case MySQLConstants.TYPE_TIME:
				columns.add(TimeColumn.valueOf(MySQLUtils.toTime(is.readInt(3))));
				break;
			case MySQLConstants.TYPE_DATETIME:
				columns.add(DatetimeColumn.valueOf(is.readLong(8)));
				break;
			case MySQLConstants.TYPE_TIMESTAMP:
				columns.add(TimestampColumn.valueOf(MySQLUtils.toTimestamp(is.readLong(4))));
				break;
			case MySQLConstants.TYPE_ENUM:
				columns.add(EnumColumn.valueOf(is.readInt(length)));
				break;
			case MySQLConstants.TYPE_SET:
				columns.add(SetColumn.valueOf(is.readLong(length)));
				break;
			case MySQLConstants.TYPE_BIT:
				final int bitLength = (meta >> 8) * 8 + (meta & 0xFF);
				columns.add(is.readBit(bitLength, false));
				break;
			case MySQLConstants.TYPE_BLOB:
				final int blobLength = is.readInt(meta);
				columns.add(BlobColumn.valueOf(is.readBytes(blobLength)));
				break;
			case MySQLConstants.TYPE_GEOMETRY:
				final int geomLength = is.readInt(meta);
				final int _unknown = is.readInt(4);
				byte[] wkb = is.readBytes(geomLength - 4);

				// using vividsolutions to parse wkb formatted data
				final WKBReader reader = new WKBReader();
				try {
					final Geometry g = reader.read(wkb);
					columns.add(GeometryColumn.valueOf(g));
				} catch (ParseException e) {
					throw new RuntimeException("Could not parse geometry, unknown column was " + _unknown, e);
				}
				break;
			case MySQLConstants.TYPE_NEWDECIMAL:
				final int precision = meta & 0xFF;
				final int scale = meta >> 8;
				final int decimalLength = MySQLUtils.getDecimalBinarySize(precision, scale);
				columns.add(DecimalColumn
						.valueOf(MySQLUtils.toDecimal(precision, scale, is.readBytes(decimalLength)), precision,
								scale));
				break;
			case MySQLConstants.TYPE_STRING:
				final int stringLength = length < 256 ? is.readInt(1) : is.readInt(2);
				columns.add(is.readFixedLengthString(stringLength));
				break;
			case MySQLConstants.TYPE_VARCHAR:
			case MySQLConstants.TYPE_VAR_STRING:
				final int varcharLength = meta < 256 ? is.readInt(1) : is.readInt(2);
				columns.add(is.readFixedLengthString(varcharLength));
				break;
			case MySQLConstants.TYPE_TIME2:
				final int value1 = is.readInt(3, false);
				final int metaLength1 = (meta + 1) / 2;
				final int nanos1 = is.readInt(metaLength1, false);
				Pair<Timestamp, String> ts = MySQLUtils.time2toTimestamp(value1, nanos1, metaLength1);
				columns.add(Time2Column.valueOf(ts.getLeft(), ts.getRight()));
				break;
			case MySQLConstants.TYPE_DATETIME2:
				final long value2 = is.readLong(5, false);
				final int metaLength2 = (meta + 1) / 2;
				final int nanos2 = is.readInt(metaLength2, false);
				Pair<Timestamp, String> ts2 = MySQLUtils.datetime2ToTimestamp(value2, nanos2, metaLength2);
				columns.add(Datetime2Column.valueOf(ts2.getLeft(), ts2.getRight()));
				break;
			case MySQLConstants.TYPE_TIMESTAMP2:
				final long value3 = is.readLong(4, false);
				final int metaLength3 = (meta + 1) / 2;
				final int nanos3 = is.readInt(metaLength3, false);
				Pair<Timestamp, String> ts2Pair = MySQLUtils.timestamp2ToTimestamp(value3, nanos3, metaLength3);
				columns.add(Timestamp2Column.valueOf(ts2Pair.getLeft(), ts2Pair.getRight()));
				break;
			case MySQLConstants.TYPE_JSON:
				final int jsonLength = is.readInt(meta);
				if (jsonLength > 0) {
					columns.add(is.readFixedLengthJsonString(jsonLength));
				} else {
					columns.add(NullColumn.valueOf(type));
				}
				break;
			default:
				throw new RuntimeException("assertion failed, unknown column type: " + type);
			}
		}
		return new Row(columns);
	}

	@Override public void skip(XInputStream is, int skipLen) throws IOException {
		long skipBytes = is.skip(skipLen);
		if (skipBytes != skipLen) {
			LOG.warn("number of bytes({}) skipped is less than expected({})", skipBytes, skipLen);
		}
	}
}
