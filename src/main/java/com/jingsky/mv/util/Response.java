package com.jingsky.mv.util;

import lombok.Data;

import java.io.Serializable;

@Data
public class Response<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    //状态码(1成功，0失败，2警告)
    private int code;
    //消息(一般失败或警告时有)
    private String msg;
    //数据体
    private T data;

    public Response(){
    }

    public Response(T data) {
        this.code = ResponseCode.SUCCESS.getCode();
        this.msg = ResponseCode.SUCCESS.getMsg();
        this.data = data;
    }

    public Response failure() {
        failure(ResponseCode.ERROR.getMsg());
        return this;
    }

    public Response failure(String msg) {
        this.code = ResponseCode.ERROR.getCode();
        this.msg = msg;
        return this;
    }

    public Response warning() {
        this.code = ResponseCode.WARNING.getCode();
        this.msg = ResponseCode.WARNING.getMsg();
        return this;
    }
}
