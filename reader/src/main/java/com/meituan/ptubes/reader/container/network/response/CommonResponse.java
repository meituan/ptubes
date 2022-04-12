package com.meituan.ptubes.reader.container.network.response;

public class CommonResponse {

    private ResponseCode code = ResponseCode.SUCCESS;
    private String msg;

    public CommonResponse() {
    }

    public ResponseCode getCode() {
        return code;
    }

    public void setCode(ResponseCode code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void fail(String msg) {
        this.code = ResponseCode.FAIL;
        this.msg = msg;
    }

    public boolean isSuccess() {
        return ResponseCode.SUCCESS.equals(this.code);
    }

    public boolean isFail() {
        return !isSuccess();
    }
}
