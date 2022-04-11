package com.meituan.ptubes.common.exception;


public class OffsetNotFoundException extends Exception{
	private static final long serialVersionUID = 1L;

	public OffsetNotFoundException() {
	}

	public OffsetNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	public OffsetNotFoundException(String message) {
		super(message);
	}

	public OffsetNotFoundException(Throwable cause) {
		super(cause);
	}
}
