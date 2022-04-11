package com.meituan.ptubes.reader.producer.mysqlreplicator.common.error;

public class ORException extends Exception {
	private static final long serialVersionUID = 1L;

	public ORException() {
	}

	public ORException(String message) {
		super(message);
	}
}
