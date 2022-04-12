package com.meituan.ptubes.reader.container.common.vo;

import java.io.Serializable;

public class Result<T> implements Serializable {

	private static final long serialVersionUID = -4610475166814365684L;

	public static <T> Result<T> create(T data, String msg, ResultCode code) {
		return new Result<T>(code, data, msg);
	}

	public static <T> Result<T> fail(String msg) {
		return create(null, msg, ResultCode.FAIL);
	}

	public static <T> Result<T> ok(T data) {
		return create(data, null, ResultCode.SUCCESS);
	}

	
	public enum ResultCode {
		SUCCESS, FAIL;
	}

	private ResultCode code;
	private String msg;
	private T data;

	public Result() {
		this.code = ResultCode.SUCCESS;
	}

	private Result(ResultCode code, T data) {
		this.code = code;
		this.data = data;
	}

	private Result(ResultCode code, T data, String msg) {
		this(code, data);
		this.msg = msg;
	}

	public ResultCode getCode() {
		return code;
	}

	public void setCode(ResultCode code) {
		this.code = code;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public void success(T data) {
		this.data = data;
		this.code = ResultCode.SUCCESS;
	}

	public Result<T> failure(String msg) {
		this.code = ResultCode.FAIL;
		this.msg = msg;
		return this;
	}

	public boolean isSuccess() {
		return (ResultCode.SUCCESS.equals(code));
	}

	public boolean isFail() {
		return !isSuccess();
	}

	@Override
	public String toString() {
		return "Result{" + "code=" + code + ", msg='" + msg + '\'' + ", data=" + data + '}';
	}
}
