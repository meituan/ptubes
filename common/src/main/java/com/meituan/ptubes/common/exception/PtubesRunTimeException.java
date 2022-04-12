package com.meituan.ptubes.common.exception;


public class PtubesRunTimeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public PtubesRunTimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public PtubesRunTimeException(String message) {
		super(message);
	}

	public PtubesRunTimeException(Throwable cause) {
		super(cause);
	}
}
