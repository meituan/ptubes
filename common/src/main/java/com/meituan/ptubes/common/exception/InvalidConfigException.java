package com.meituan.ptubes.common.exception;

public class InvalidConfigException extends PtubesException {
	private static final long serialVersionUID = 1L;

	public InvalidConfigException(String msg) {
		super(msg);
	}

	public InvalidConfigException(Throwable cause) {
		super(cause);
	}

	public InvalidConfigException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
