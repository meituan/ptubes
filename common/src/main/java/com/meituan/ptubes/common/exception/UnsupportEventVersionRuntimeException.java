package com.meituan.ptubes.common.exception;

public class UnsupportEventVersionRuntimeException extends PtubesRunTimeException {
	public UnsupportEventVersionRuntimeException(byte version) {
		super("Unsupported PtubesEvent version " + version);
	}
}
