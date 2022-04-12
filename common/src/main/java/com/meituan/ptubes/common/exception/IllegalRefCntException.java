package com.meituan.ptubes.common.exception;

public class IllegalRefCntException extends IllegalStateException {

	private static final long serialVersionUID = -1862884485273095635L;

	public IllegalRefCntException() { }

	public IllegalRefCntException(int refCnt) {
		this("refCnt: " + refCnt);
	}

	public IllegalRefCntException(int refCnt, int increment) {
		this("refCnt: " + refCnt + ", " + (increment > 0? "increment: " + increment : "decrement: " + -increment));
	}

	public IllegalRefCntException(String message) {
		super(message);
	}

	public IllegalRefCntException(String message, Throwable cause) {
		super(message, cause);
	}

	public IllegalRefCntException(Throwable cause) {
		super(cause);
	}

}
