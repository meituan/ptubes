package com.meituan.ptubes.common.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.meituan.ptubes.common.exception.PtubesRunTimeException;


public final class DateUtil {
	private static final ThreadLocal<SimpleDateFormat> DATE_HOUR_FORMATTER = new ThreadLocal<SimpleDateFormat>() {
		@Override
        protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyyMMddHH");
		}
	};

	private static final ThreadLocal<SimpleDateFormat> HOUR_FORMATTER = new ThreadLocal<SimpleDateFormat>() {
		@Override
        protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("HH");
		}
	};

	public static String getCurrentDateHourString() {
		return DATE_HOUR_FORMATTER.get().format(new Date());
	}

	public static int getCurrentDateInteger() {
		return Integer.parseInt(getCurrentDateHourString());
	}

	public static int getCurrentHourInteger() {
		return Integer.parseInt(HOUR_FORMATTER.get().format(new Date()));
	}

	public static String getNextHour(String dateHourStr) {
		long ts = 0;
		try {
			ts = DATE_HOUR_FORMATTER.get().parse(dateHourStr).getTime() + 1 * 3600 * 1000;
		} catch (ParseException e) {
			throw new PtubesRunTimeException(e);
		}
		return DATE_HOUR_FORMATTER.get().format(new Date(ts));
	}

	public static boolean isExpired(
		String dateHourStr,
		int expireTimeInHour
	) {
		long ts = 0L;
		long current = 0L;
		try {
			ts = DATE_HOUR_FORMATTER.get()
				.parse(dateHourStr)
				.getTime() / 1000;
			current = System.currentTimeMillis() / 1000;
			return current / (60 * 60) - ts / (60 * 60) >= expireTimeInHour;
		} catch (ParseException e) {
			return false;
		}
	}

	public static int getDateHour(long timestamp) {
		String dateStr = DATE_HOUR_FORMATTER.get().format(new Date(timestamp));
		return Integer.parseInt(dateStr);
	}

}
