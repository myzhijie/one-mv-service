package com.jingsky.mv.util;

public enum ResponseCode {
	ERROR(0, "失败"),
	SUCCESS(1, "成功"),
	WARNING(2, "警告");

	private final Integer code;
	private String msg;

	ResponseCode(final Integer code, final String msg) {
		this.code = code;
		this.msg = msg;
	}

	public Integer getCode() {
		return code;
	}

	public String getMsg() {
		return msg;
	}
}
