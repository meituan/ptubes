package com.meituan.ptubes.common.utils;

import java.nio.charset.Charset;


public class SerializeUtil {
	public static final String DEFAULT_ENCODING_NAME = "UTF-8";

	public static String getString(byte[] bytes) {
		return new String(bytes, Charset.forName(DEFAULT_ENCODING_NAME));
	}

	public static byte[] getBytes(String str) {
		return str.getBytes(Charset.forName(DEFAULT_ENCODING_NAME));
	}

}
