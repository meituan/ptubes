package com.meituan.ptubes.common.exception;


public class GreaterThanStorageRangeException extends Exception {
	public GreaterThanStorageRangeException() {
		super();
	}

	public GreaterThanStorageRangeException(String message, Throwable cause) {
		super(message, cause);
	}

	public GreaterThanStorageRangeException(String message) {
		super(message);
	}

	public GreaterThanStorageRangeException(Throwable cause) {
		super(cause);
	}
}
