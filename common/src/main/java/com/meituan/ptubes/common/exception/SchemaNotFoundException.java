package com.meituan.ptubes.common.exception;

public class SchemaNotFoundException extends Exception {

	private static final long serialVersionUID = 3454043231112284680L;

	public SchemaNotFoundException() { }

	public SchemaNotFoundException(String message) {
		super(message);
	}

	public SchemaNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	public SchemaNotFoundException(Throwable cause) {
		super(cause);
	}

}
