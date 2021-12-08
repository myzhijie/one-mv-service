package com.jingsky.mv.util;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;

@ApiModel("返回体")
@Data
public class Response<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    @ApiModelProperty(
            value = "状态码(1成功，0失败，2警告)",
            example = "1"
    )
    private int code;
    @ApiModelProperty(
            value = "消息(一般失败或警告时有)",
            example = "1"
    )
    private String msg;
    @ApiModelProperty(
            value = "数据体",
            example = "{name:123}"
    )
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
