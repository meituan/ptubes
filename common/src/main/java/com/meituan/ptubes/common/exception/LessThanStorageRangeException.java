package com.meituan.ptubes.common.exception;


public class LessThanStorageRangeException extends Exception {
	public LessThanStorageRangeException() {
		super();
	}

	public LessThanStorageRangeException(String message, Throwable cause) {
		super(message, cause);
	}

	public LessThanStorageRangeException(String message) {
		super(message);
	}

	public LessThanStorageRangeException(Throwable cause) {
		super(cause);
	}
}
