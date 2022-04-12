package com.meituan.ptubes.common.utils;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {
	private static final Logger LOG = LoggerFactory.getLogger(TimeUtil.class);
	private static final long BASE_REALTIME = System.currentTimeMillis() * 1000000;
	private static final long BASE_NANOTIME = System.nanoTime();

	/** Current time with nano-second granularity */
	public static long currentNanoTime() {
		return (System.nanoTime() - BASE_NANOTIME) + BASE_REALTIME;
	}

	public static ThreadLocal<SimpleDateFormat> timeStrFormater = new ThreadLocal<SimpleDateFormat>() {
		@Override
		public SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
	};

	public static String timeStr(long time) {
		try {
			if (Long.toString(time).length() > 13) {
				String timeStr = Long.toString(time).substring(0, 13);
				try {
					time = Long.parseLong(timeStr);
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
				}
			}
			return timeStrFormater.get().format(new Date(time));
		} catch (Exception e) {
			LOG.error("time conversion failed!", e);
			return "0000-00-00 00:00:00";
		}
	}

	public static Date str2Time(String timeStr) throws ParseException {
		return timeStrFormater.get().parse(timeStr);
	}

	public static long str2Timestamp(String timeStr) throws ParseException {
		return timeStrFormater.get().parse(timeStr).getTime();
	}

	public static boolean timeIsOk(long timestamp) {
		try {
			String timeStr = timeStr(timestamp);
			return timeStr.length() == 19;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return false;
		}
	}
}
