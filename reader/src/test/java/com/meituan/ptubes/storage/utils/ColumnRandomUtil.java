package com.meituan.ptubes.storage.utils;

import org.apache.commons.lang3.RandomUtils;


public class ColumnRandomUtil {

	public static Object getTinyColumn() {
		return RandomUtils.nextInt(0, 127);
	}

	public static Object getShortColumn() {
		return RandomUtils.nextInt(0, Short.MAX_VALUE);
	}

	public static Object getInt24Column() {
		return RandomUtils.nextInt(0, 8388607);
	}

	public static Object getLongColumn() {
		return RandomUtils.nextInt(0, Integer.MAX_VALUE);
	}

	public static Object getLongLongColumn() {
		return RandomUtils.nextLong(0, Long.MAX_VALUE);
	}

	public static Object getFloatColumn() {
		return RandomUtils.nextFloat(0, Float.MAX_VALUE);
	}

	public static Object getDoubleColumn() {
		return RandomUtils.nextDouble(0, Integer.MAX_VALUE);
	}

	public static Object getYearColumn() {
		return RandomUtils.nextInt(1901, 2155);
	}

	public static Object getStringColumn() {
//		return new String(RandomUtils.nextBytes(RandomUtils.nextInt(1, 100)));
		return "test";
	}

	public static Object getJSONColumn() {
		return "{\"name\":1}";
	}

	public static Object getTimestamp2Column() {
		return System.currentTimeMillis();
	}

}
