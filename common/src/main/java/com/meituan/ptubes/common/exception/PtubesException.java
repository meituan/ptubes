package com.meituan.ptubes.common.exception;


public class PtubesException extends Exception {
	private static final long serialVersionUID = 1L;
	private boolean needPush = true;

	public PtubesException() {
		super();
	}

	public PtubesException(String message, Throwable cause) {
		super(message, cause);
	}

	public PtubesException(String message) {
		super(message);
	}

	public PtubesException(Throwable cause) {
		super(cause);
	}

	public PtubesException withoutPush() {
		this.needPush = false;
		return this;
	}

	public boolean isNeedPush() {
		return needPush;
	}
}
