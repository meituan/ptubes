package com.meituan.ptubes.common.exception;


public class RdsCdcRuntimeException extends RuntimeException {

    public RdsCdcRuntimeException() {
        super();
    }

    public RdsCdcRuntimeException(String message) {
        super(message);
    }

    public RdsCdcRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RdsCdcRuntimeException(Throwable cause) {
        super(cause);
    }
}
