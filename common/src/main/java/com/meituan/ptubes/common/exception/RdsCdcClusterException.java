package com.meituan.ptubes.common.exception;

@SuppressWarnings("serial")
public class RdsCdcClusterException extends Exception {

    public RdsCdcClusterException(String msg) {
        super(msg);
    }

    public RdsCdcClusterException(Throwable cause) {
        super(cause);
    }
}
