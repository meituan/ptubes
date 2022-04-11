/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.meituan.ptubes.reader.producer.mysqlreplicator.utils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Calendar;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public final class MySQLUtils {
	//
	private static final int DIGITS_PER_4BYTES = 9;
	private static final BigDecimal POSITIVE_ONE = BigDecimal.ONE;
	private static final BigDecimal NEGATIVE_ONE = new BigDecimal("-1");
	private static final int[] DECIMAL_BINARY_SIZE = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

	public static byte[] password41OrLater(byte[] password, byte[] scramble) {
		final byte[] stage1 = CodecUtils.sha(password);
		final byte[] stage2 = CodecUtils.sha(CodecUtils.concat(scramble, CodecUtils.sha(stage1)));
		return CodecUtils.xor(stage1, stage2);
	}

	public static int toYear(int value) {
		return 1900 + value;
	}

	/**
	 * see: https://dev.mysql.com/doc/refman/5.7/en/datetime.html，date range: ['1000-01-01','9999-12-31']
	 * Invalid DATE, DATETIME, or TIMESTAMP values are converted to the “zero” value of the appropriate type ('0000-00-00' or '0000-00-00 00:00:00')
	 *
	 * @param value encoded date value
	 * 	 			* 23 bits year (0-9999)
	 * 	 			* 4 bits month (1-12)
	 * 	 			* 5 bits day (1-31)
	 * @return
	 */
	public static Pair<DateTime, String> toDate(int value) {
		final int d = value % 32;
		value >>>= 5;
		final int m = value % 16;
		final int y = value >> 4;

		boolean backup = false;
		DateTime millisDate = null;
		String stringDate = null;
		try {
			if (y >= 1000) {
				millisDate = new DateTime(y, m, d, 0, 0, 0);
			} else {
				backup = true;
			}
		} catch (Throwable te) {
			backup = true;
		}

		if (backup) {
			final Calendar cal = Calendar.getInstance();
			cal.clear();
			cal.set(y, m - 1, d);
			millisDate = new DateTime(cal.getTimeInMillis());
		}
		stringDate = String.format("%04d-%02d-%02d", y, m, d);
		return Pair.of(millisDate, stringDate);
	}

	public static java.sql.Time toTime(int value) {
		final int s = (int) (value % 100);
		value /= 100;
		final int m = (int) (value % 100);
		final int h = (int) (value / 100);
		final Calendar c = Calendar.getInstance();
		c.set(1970, 0, 1, h, m, s);
		c.set(Calendar.MILLISECOND, 0);
		return new java.sql.Time(c.getTimeInMillis());
	}

	/**
	 *
	 * @param negative true: complement form, false: true form
	 * 	 				* 1 bit sign (1 = non-negative, 0 = negative)
	 * 	 				* 1 bit unused (reserved for future extensions)
	 * 	 				* 10 bits hour (0-838)
	 * 	 				* 6 bits minute (0-59)
	 * 	 				* 6 bits second (0-59) (LSB)
	 * @param value encoded time
	 * @return
	 */
	public static Pair<Long, String> getMillisFromTime2(boolean negative, int value) {
		int originValue = value & 0x3FFFFF;
		if (negative) {
			originValue = (~(originValue - 1) & 0x3FFFFF);
		}
		int h = (originValue >> 12) & 0x3FF;
		int m = (originValue >> 6) & 0x3F;
		int s = (originValue >> 0) & 0x3F;

		DateTime dateTime;
		if (negative == false) {
			dateTime = new DateTime(1970, 1, 1, 0, 0, 0).withZone(DateTimeZone.UTC).plusHours(h).plusMinutes(m).plusSeconds(s);
		} else {
			dateTime = new DateTime(1970, 1, 1, 0, 0, 0).withZone(DateTimeZone.UTC).plusHours(-h).plusMinutes(-m).plusSeconds(-s);
		}
		String stringTime = String.format((negative ? "-%02d:%02d:%02d" : "%02d:%02d:%02d"), h, m, s);
		return Pair.of(dateTime.getMillis(), stringTime);
	}

	public static Pair<Timestamp, String> time2toTimestamp(int value, int fraction, int width) {
		final boolean negative = ((value >> 23) & 0x01) == 0;
		final Pair<Long, String> millis = getMillisFromTime2(negative, value);
		final Timestamp t = new Timestamp(millis.getLeft());
		if (negative) {
			fraction = (~(fraction - 1) & 0xFFFFF);
		}
		int nanos = nanosForFractionalValue(fraction, width);
		t.setNanos(nanos);
		String stringValue = (StringUtils.isBlank(millis.getRight()) ? millis.getRight() : String.format("%s.%06d", millis.getRight(), nanos/1000));
		return Pair.of(t, stringValue);
	}

	// fractional values are 0 - 3 bytes wide.
	// 1 byte, we get 0-99 resolution
	// 2 bytes, we get 0-9,999
	// 3 bytes, we get 0-999,999 resolution

	// 1 byte: the value "99" means 990,000,000 nanos == fraction * 10^7
	// 2 bytes: the value "9,999" means 999,900,000 nanos == fraction * 10^5
	// 3 bytes: the value "999,999" means "999,999,000" nanos == fraction * 10^3
	public static int nanosForFractionalValue(int value, int width) {
		switch (width) {
		case 0:
			return 0;
		case 1:
			return value * 10000000;
		case 2:
			return value * 100000;
		case 3:
			return value * 1000;
		default:
			throw new RuntimeException("unexpected number of fractional bytes");
		}
	}

	public static java.util.Date toDatetime(long value) {
		final int second = (int) (value % 100);
		value /= 100;
		final int minute = (int) (value % 100);
		value /= 100;
		final int hour = (int) (value % 100);
		value /= 100;
		final int day = (int) (value % 100);
		value /= 100;
		final int month = (int) (value % 100);
		final int year = (int) (value / 100);
		final Calendar c = Calendar.getInstance();
		c.set(year, month - 1, day, hour, minute, second);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTime();
	}

	/**
	 * see: https://dev.mysql.com/doc/refman/5.7/en/datetime.html, datetime range: ['1000-01-01 00:00:00.000000','9999-12-31 23:59:59.999999']
	 *
	 * @param value encoded datetime value
	 * @return Pair.left: timestamp, Pair.right: formatted datetime as 'yyyy-MM-dd HH:mm:ss.SSSSSS'
	 */
	public static Pair<Long, String> getMillisFromDatetime2(long value) {
		final long x = (value >> 22) & 0x1FFFFL;
		final int year = (int) (x / 13);
		final int month = (int) (x % 13);
		final int day = ((int) (value >> 17)) & 0x1F;
		final int hour = ((int) (value >> 12)) & 0x1F;
		final int minute = ((int) (value >> 6)) & 0x3F;
		final int second = ((int) (value >> 0)) & 0x3F;

		boolean backup = false;
		DateTime millisDateTime = null;
		String stringDateTime = null;
		try {
			if (year >= 1000) {
				millisDateTime = new DateTime(year, month, day, hour, minute, second);
			} else {
				backup = true;
			}
		} catch (Throwable te) {
			backup = true;
		}

		if (backup) {
			final Calendar c = Calendar.getInstance();
			c.set(year, month - 1, day, hour, minute, second);
			c.set(Calendar.MILLISECOND, 0);
			millisDateTime = new DateTime(c.getTimeInMillis());
		}
		stringDateTime = String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
		return Pair.of(millisDateTime.getMillis(), stringDateTime);
	}

	public static java.sql.Timestamp toTimestamp(long seconds) {
		return new Timestamp(seconds * 1000L);
	}

	public static Pair<Timestamp, String> timestamp2ToTimestamp(long seconds, int fraction, int width) {
		final Timestamp r = new Timestamp(seconds * 1000L);
		r.setNanos(nanosForFractionalValue(fraction, width));
		if (seconds > 0) {
			DateTime dateTime = new DateTime(r.getTime());
			return Pair.of(r, String.format("%s.%06d", dateTime.toString("yyyy-MM-dd HH:mm:ss"), r.getNanos() / 1000));
		} else {
			return Pair.of(r, "0000-00-00 00:00:00.000000");
		}
	}

	public static Pair<Timestamp, String> datetime2ToTimestamp(long value, int fraction, int width) {
		final Pair<Long, String> millis = getMillisFromDatetime2(value);
		final Timestamp r = new Timestamp(millis.getLeft());
		int nanos = nanosForFractionalValue(fraction, width);
		r.setNanos(nanos);
		String stringValue = String.format("%s.%06d", millis.getRight(), nanos / 1000);
		return Pair.of(r, stringValue);
	}

	public static BigDecimal toDecimal(int precision, int scale, byte[] value) {
		//
		final boolean positive = (value[0] & 0x80) == 0x80;
		value[0] ^= 0x80;
		if (!positive) {
			for (int i = 0; i < value.length; i++) {
				value[i] ^= 0xFF;
			}
		}

		//
		final int x = precision - scale;
		final int ipDigits = x / DIGITS_PER_4BYTES;
		final int ipDigitsX = x - ipDigits * DIGITS_PER_4BYTES;
		final int ipSize = (ipDigits << 2) + DECIMAL_BINARY_SIZE[ipDigitsX];
		int offset = DECIMAL_BINARY_SIZE[ipDigitsX];
		BigDecimal ip = offset > 0 ? BigDecimal.valueOf(CodecUtils.toInt(value, 0, offset)) : BigDecimal.ZERO;
		for (; offset < ipSize; offset += 4) {
			final int i = CodecUtils.toInt(value, offset, 4);
			ip = ip.movePointRight(DIGITS_PER_4BYTES).add(BigDecimal.valueOf(i));
		}

		//
		int shift = 0;
		BigDecimal fp = BigDecimal.ZERO;
		for (; shift + DIGITS_PER_4BYTES <= scale; shift += DIGITS_PER_4BYTES, offset += 4) {
			final int i = CodecUtils.toInt(value, offset, 4);
			fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + DIGITS_PER_4BYTES));
		}
		if (shift < scale) {
			final int i = CodecUtils.toInt(value, offset, DECIMAL_BINARY_SIZE[scale - shift]);
			fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
		}

		//
		return positive ? POSITIVE_ONE.multiply(ip.add(fp)) : NEGATIVE_ONE.multiply(ip.add(fp));
	}

	public static int getDecimalBinarySize(int precision, int scale) {
		final int x = precision - scale;
		final int ipDigits = x / DIGITS_PER_4BYTES;
		final int fpDigits = scale / DIGITS_PER_4BYTES;
		final int ipDigitsX = x - ipDigits * DIGITS_PER_4BYTES;
		final int fpDigitsX = scale - fpDigits * DIGITS_PER_4BYTES;
		return (ipDigits << 2) + DECIMAL_BINARY_SIZE[ipDigitsX] + (fpDigits << 2) + DECIMAL_BINARY_SIZE[fpDigitsX];
	}
}
