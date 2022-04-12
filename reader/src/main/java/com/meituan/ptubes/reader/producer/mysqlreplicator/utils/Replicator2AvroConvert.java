package com.meituan.ptubes.reader.producer.mysqlreplicator.utils;

import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.DateColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Datetime2Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Time2Column;
import java.sql.Timestamp;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;

import com.meituan.ptubes.common.exception.PtubesException;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.DatetimeColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.GeometryColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Int24Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.LongColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.LongLongColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.NullColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.ShortColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.TimeColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Timestamp2Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.TimestampColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.TinyColumn;
import com.meituan.ptubes.reader.schema.common.FieldMeta;
import com.meituan.ptubes.reader.schema.util.SchemaPrimitiveTypes;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;

public class Replicator2AvroConvert {
	public static final int TINYINT_MAX_VALUE = 256;
	public static final int SMALLINT_MAX_VALUE = 65536;
	public static final int MEDIUMINT_MAX_VALUE = 16777216;
	public static final long INTEGER_MAX_VALUE = 4294967296L;
	public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");

	public static Object convert(Column s, FieldMeta fieldMeta) throws PtubesException {
		if (s instanceof NullColumn) {
			return null;
		}

		SchemaPrimitiveTypes schemaPrimitiveTypes = fieldMeta.getPrimitiveType();
		switch (schemaPrimitiveTypes.getPtubesFieldType()) {
			case INT:
				return IntegerConverter.converter.convert(s, fieldMeta);
			case LONG:
				return LongConverter.converter.convert(s, fieldMeta);
			case DOUBLE:
				return DoubleConverter.converter.convert(s, fieldMeta);
			case STRING:
				return StringConverter.converter.convert(s, fieldMeta);
			case BYTES:
				return BytesConverter.converter.convert(s, fieldMeta);
			case FLOAT:
				return FloatConverter.converter.convert(s, fieldMeta);
			case DATE:
			case DATETIME:
			case TIME:
			case TIMESTAMP:
				return TimestampConverter.CONVERTER.convert(s, fieldMeta);
			case GEOMETRY:
				return GeometryConverter.CONVERTER.convert(s, fieldMeta);
			default:
				throw new PtubesException("Schema: " + schemaPrimitiveTypes.name() + " has no converter");
		}
	}

	public static abstract class Or2AvroBasicConvert {
		public abstract Object convert(Column s, FieldMeta fieldMetam)
				throws PtubesException;
	}

	public static class GeometryConverter extends Or2AvroBasicConvert {
		public static final GeometryConverter CONVERTER = new GeometryConverter();

		@Override public Object convert(Column s, FieldMeta fieldMetam) throws PtubesException {
			Object obj = s.getValue();
			if (s instanceof GeometryColumn) {
				return ((GeometryColumn) s).getValue().toText();
			} else {
				throw new PtubesException((obj == null ? "" : (s.getClass() + " | " + obj.getClass() + " | " + obj)) + " can not convert to Geometry");
			}
		}
	}

	public static class TimestampConverter extends Or2AvroBasicConvert {
		public static final TimestampConverter CONVERTER = new TimestampConverter();

		@Override
		public Pair<Timestamp, String> convert(Column s, FieldMeta fieldMeta) throws PtubesException {
			Object obj = s.getValue();
			if (s instanceof DateColumn) {
				DateTime dt = ((DateColumn) s).getValue();
				String stringValue = ((DateColumn) s).getStringValue();
				Timestamp date2Ts = new Timestamp(dt.getMillis());
				return Pair.of(date2Ts, stringValue);
			} else if (s instanceof Datetime2Column) {
				Timestamp dateTime2Ts = ((Datetime2Column) s).getTimestampValue();
				String invalidDateTime2 = ((Datetime2Column) s).getStringValue();
				return Pair.of(dateTime2Ts, invalidDateTime2);
			} else if (s instanceof Time2Column) {
				Timestamp time2Ts = ((Time2Column) s).getTimestampValue();
				String time2String = ((Time2Column) s).getStringValue();
				return Pair.of(time2Ts, time2String);
			} else if (s instanceof Timestamp2Column) {
				Timestamp ts = ((Timestamp2Column) s).getValue();
				String invalidTs = ((Timestamp2Column) s).getStringValue();
				return Pair.of(ts, invalidTs);
			} else if (s instanceof TimestampColumn) {
				return Pair.of(((TimestampColumn) s).getValue(), null);
			} else if (s instanceof TimeColumn) {
				return Pair.of(new Timestamp(((TimeColumn) s).getValue().getTime()), null);
			} else if (s instanceof DatetimeColumn) {
				return Pair.of(new Timestamp(((DatetimeColumn) s).getLongValue()), null);
			} else {
				throw new PtubesException((obj == null ? "" : (s.getClass() + " | " + obj.getClass() + " | " + obj)) + " can not convert to Timestamp");
			}
		}
	}

	public static class IntegerConverter extends Or2AvroBasicConvert {
		public static IntegerConverter converter = new IntegerConverter();

		@Override
		public Object convert(Column s, FieldMeta fieldMeta) throws PtubesException {
			Object obj = s.getValue();
			Integer res = null;
			if (obj instanceof Number) {
				res = ((Number) obj).intValue();
			} else if (obj instanceof byte[]) {
				String resStr = new String((byte[]) obj, Charset.defaultCharset());
				res = Integer.valueOf(resStr);
			} else {
				throw new PtubesException((obj == null ? "" : (obj.getClass() + " | " + obj)) + " can not convert to Integer");
			}
			return res.intValue() + (int) unsignedOffset(s, fieldMeta);
		}
	}

	public static class LongConverter extends Or2AvroBasicConvert {
		public static LongConverter converter = new LongConverter();

		@Override
		public Object convert(Column s, FieldMeta fieldMeta) throws PtubesException {
			Object obj = s.getValue();
			Long res = null;
			if (obj instanceof Date) {
				return ((Date) obj).getTime();
			} else if (obj instanceof Number) {
				res = ((Number) obj).longValue();
			} else if (obj instanceof byte[]) {
				String resStr = new String((byte[]) obj, Charset.defaultCharset());
				res = Long.valueOf(resStr);
			} else {
				throw new PtubesException((obj == null ? "" : (obj.getClass() + " | " + obj)) + " can not convert to Long");
			}
			return res.longValue() + unsignedOffset(s, fieldMeta);
		}
	}

	public static class DoubleConverter extends Or2AvroBasicConvert {
		public static DoubleConverter converter = new DoubleConverter();

		@Override
		public Object convert(Column s, FieldMeta fieldMeta) throws PtubesException {
			Object obj = s.getValue();
			Double res = null;
			if (obj instanceof Number) {
				res = ((Number) obj).doubleValue();
			} else if (obj instanceof byte[]) {
				String resStr = new String((byte[]) obj, Charset.defaultCharset());
				res = Double.valueOf(resStr);
			} else {
				throw new PtubesException((obj == null ? "" : (obj.getClass() + " | " + obj)) + " can not convert to Double");
			}
			return res;
		}
	}

	public static class StringConverter extends Or2AvroBasicConvert {
		public static StringConverter converter = new StringConverter();

		@Override
		public Object convert(Column s, FieldMeta fieldMeta) {
			if (s.getValue() instanceof byte[]) {
				return new String((byte[]) s.getValue(), Charset.defaultCharset());
			}
			return s.getValue().toString();//If null, it should be nullColumn. so s.getValue will not be null
		}
	}

	public static class BytesConverter extends Or2AvroBasicConvert {
		public static BytesConverter converter = new BytesConverter();

		@Override
		public Object convert(Column s, FieldMeta fieldMeta) throws PtubesException {
			if (!(s.getValue() instanceof byte[])) {
				throw new PtubesException("Can not convert to bytes");
			}
			byte[] byteArr = (byte[]) s.getValue();
			return ByteBuffer.wrap(byteArr);
		}
	}

	public static class FloatConverter extends Or2AvroBasicConvert {
		public static FloatConverter converter = new FloatConverter();

		@Override
		public Object convert(Column s, FieldMeta fieldMeta) throws PtubesException {
			Object obj = s.getValue();
			Float res = null;
			if (obj instanceof Number) {
				res = ((Number) obj).floatValue();
			} else if (obj instanceof byte[]) {
				String resStr = new String((byte[]) obj, Charset.defaultCharset());
				res = Float.valueOf(resStr);
			} else {
				throw new PtubesException((obj == null ? "" : (obj.getClass() + " | " + obj)) + " can not convert to Float");
			}
			return res;
		}
	}

	private final static long unsignedOffset(Column s, FieldMeta fieldMeta) {
		if (s instanceof Int24Column) {
			Int24Column lc = (Int24Column) s;
			int i = lc.getValue().intValue();
			if (i < 0 && fieldMeta.isUnsigned()) {
				return MEDIUMINT_MAX_VALUE;
			}
			return 0;
		} else if (s instanceof LongColumn) {
			LongColumn lc = (LongColumn) s;
			Long i = lc.getValue().longValue();
			if (i < 0 && fieldMeta.isUnsigned()) {
				return INTEGER_MAX_VALUE;
			}
			return 0;
		} else if (s instanceof LongLongColumn) {
			LongLongColumn llc = (LongLongColumn) s;
			Long l = llc.getValue();
			if (l < 0 && fieldMeta.isUnsigned()) {
				return BIGINT_MAX_VALUE.longValue();
			}
			return 0;
		} else if (s instanceof ShortColumn) {
			ShortColumn sc = (ShortColumn) s;
			Integer i = sc.getValue();
			if (i < 0 && fieldMeta.isUnsigned()) {
				return SMALLINT_MAX_VALUE;
			}
			return 0;
		} else if (s instanceof TinyColumn) {
			TinyColumn tc = (TinyColumn) s;
			Integer i = tc.getValue();
			if (i < 0 && fieldMeta.isUnsigned()) {
				return TINYINT_MAX_VALUE;
			}
			return 0;
		}
		return 0;

	}
}
