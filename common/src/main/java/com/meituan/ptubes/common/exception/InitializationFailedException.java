package com.meituan.ptubes.common.exception;

public class InitializationFailedException extends RuntimeException {

	private static final long serialVersionUID = 2744695478963820135L;

	public InitializationFailedException() { }

	public InitializationFailedException(String message) {
		super(message);
	}

	public InitializationFailedException(String message, Throwable cause) {
		super(message, cause);
	}

	public InitializationFailedException(Throwable cause) {
		super(cause);
	}

}
