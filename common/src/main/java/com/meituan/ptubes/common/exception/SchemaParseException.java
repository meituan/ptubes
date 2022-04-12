package com.meituan.ptubes.common.exception;

public class SchemaParseException extends Exception {

    private static final long serialVersionUID = -2448520752224685747L;

    public SchemaParseException() { }

    public SchemaParseException(String message) {
        super(message);
    }

    public SchemaParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchemaParseException(Throwable cause) {
        super(cause);
    }

}
