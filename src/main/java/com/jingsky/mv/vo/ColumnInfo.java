package com.jingsky.mv.vo;

import lombok.Data;

@Data
public class ColumnInfo {
    //字段名
    private String Field;
    //字段类型 如：varchar(32)
    private String Type;
    //是否可为null YES或NO
    private String Null;
    //是否为主键 PRI或空字符串
    private String Key;
    private String Default;
    private String Extra;
    //字符集
    private String Collation;
    //字段备注
    private String Comment;
}
