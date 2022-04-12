package com.meituan.ptubes.common.exception;


public class UnsupportedKeyException extends Exception {
	private static final long serialVersionUID = 1L;

	public UnsupportedKeyException() {
	}

	public UnsupportedKeyException(String message, Throwable cause) {
		super(message, cause);
	}

	public UnsupportedKeyException(String message) {
		super(message);
	}

	public UnsupportedKeyException(Throwable cause) {
		super(cause);
	}
}
