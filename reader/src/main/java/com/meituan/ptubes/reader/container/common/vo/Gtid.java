package com.meituan.ptubes.reader.container.common.vo;


public class Gtid {
	private byte[] uuid;
	private long transactionId;

	public Gtid(String uuid, long transactionId) {
		this.uuid = getUuidByte(uuid);
		this.transactionId = transactionId;
	}

	public Gtid(Gtid gtid) {
		this.uuid = gtid.getUuid();
		this.transactionId = gtid.getTransactionId();
	}

	public Gtid(String gtid) {
		String[] gtidStrs = gtid.split(":");
		this.uuid = getUuidByte(gtidStrs[0].trim());
		this.transactionId = Long.parseLong(gtidStrs[1]);
	}

	public Gtid(byte[] uuid, long transactionId) {
		this.uuid = uuid;
		this.transactionId = transactionId;
	}

	public byte[] getUuid() {
		return uuid;
	}

	public void setUuid(byte[] uuid) {
		this.uuid = uuid;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(long transactionId) {
		this.transactionId = transactionId;
	}

	@Override
	public String toString() {
		return getUuidStr(this.uuid) + ":" + this.transactionId;
	}

	public static String getUuidStrOld(byte[] bytes) {
		StringBuffer sb = new StringBuffer(36);
		sb.append(byteArrayToHexOld(bytes, 0, 4))
			.append("-").append(byteArrayToHexOld(bytes, 4, 2))
			.append("-").append(byteArrayToHexOld(bytes, 6, 2))
			.append("-").append(byteArrayToHexOld(bytes, 8, 2))
			.append("-").append(byteArrayToHexOld(bytes, 10, 6));
		return sb.toString();
	}
	private static String byteArrayToHexOld(byte[] a, int offset, int len) {
		StringBuilder sb = new StringBuilder();
		for (int idx = offset; idx < (offset + len) && idx < a.length; idx++) {
			sb.append(String.format("%02x", a[idx] & 0xff));
		}
		return sb.toString();
	}

	public static String getUuidStr(byte[] bytes) {
		StringBuffer sb = new StringBuffer(36);
		sb.append(byteArrayToHex(bytes, 0, 4))
			.append("-").append(byteArrayToHex(bytes, 4, 2))
			.append("-").append(byteArrayToHex(bytes, 6, 2))
			.append("-").append(byteArrayToHex(bytes, 8, 2))
			.append("-").append(byteArrayToHex(bytes, 10, 6));
		return sb.toString();
	}
	private static final char[] TO_CHAR_ARRAY = "0123456789abcdef".toCharArray();
	private static String byteArrayToHex(byte[] a, int offset, int len) {
		StringBuilder res = new StringBuilder();
		int start = offset;
		int end = offset + len;
		end = Math.min(a.length, end);
		for (int i = start; i < end; ++i) {
			res.append(TO_CHAR_ARRAY[(a[i] & 0xF0) >>> 4]);
			res.append(TO_CHAR_ARRAY[a[i] & 0x0F]);
		}
		return res.toString();
	}

	public static byte[] getUuidByte(String uuid) {
		uuid = uuid.replace("-", "");
		byte[] b = new byte[uuid.length() / 2];
		for (int i = 0, j = 0; j < uuid.length(); j += 2) {
			b[i++] = (byte) Integer.parseInt(uuid.charAt(j) + "" + uuid.charAt(j + 1), 16);
		}
		return b;
	}
}
