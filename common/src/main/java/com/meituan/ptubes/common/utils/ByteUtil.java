package com.meituan.ptubes.common.utils;


public class ByteUtil {
	public static String byteArr2HexStr(byte[] byteArr) {
		StringBuilder sb = new StringBuilder();
		for (byte b : byteArr) {
			String str = Integer.toHexString(b & 0xFF);
			if (str.length() < 2) {
				sb.append("0");
			}
			sb.append(str);
		}
		return sb.toString();
	}

	public static byte[] hexStringToBytes(String hexString) {
		if (hexString == null || "".equals(hexString)) {
			return null;
		}
		hexString = hexString.toUpperCase();
		// Scenarios with length == 1 may be problematic
		int length = hexString.length() / 2;
		char[] hexChars = hexString.toCharArray();
		byte[] d = new byte[length];
		for (int i = 0; i < length; i++) {
			int pos = i * 2;
			d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
		}
		return d;
	}

	public static byte charToByte(char c) {
		return (byte) "0123456789ABCDEF".indexOf(c);
	}
}
