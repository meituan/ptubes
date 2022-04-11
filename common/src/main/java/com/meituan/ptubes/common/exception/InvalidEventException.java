package com.meituan.ptubes.common.exception;


public class InvalidEventException extends Exception {
	private static final long serialVersionUID = -2563643656130857310L;

	public InvalidEventException() {
		super();
	}

	public InvalidEventException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidEventException(String message) {
		super(message);
	}

	public InvalidEventException(Throwable cause) {
		super(cause);
	}

}
