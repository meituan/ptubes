package com.meituan.ptubes.reader.container.common.constants;

public enum ErrorCode {

	COMMUNICATION_TIMING_ERROR(-1),
	SERVER_BUSY(-2),
	SERVER_INTERNAL_ERROR(-100);

	private int value;
	ErrorCode(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}
}
