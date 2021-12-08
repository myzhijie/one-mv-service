package com.jingsky.mv.entity;

import lombok.Data;
import java.io.Serializable;
import java.util.Date;

/**
 * 视图定义表
 */
@Data
public class TableView implements Serializable {
    private static final long serialVersionUID = 1169510011615959911L;

    private Integer id;
    //视图名称
    private String mvName;
    //列的对应json,{'目标列名':'源表名.字段名'}
    private String colsJson;
    //主表表名
    private String masterTable;
    //主表的主键，必须在目标列中存在映射
    private String masterTablePk;
    //主表where语句
    private String whereSql;
    //left join部分,{[table:'table1',where:'where sql',on:{left:'a字段',right:'b字段'}}],}
    private String leftJoinJson;
    //一个帮助你放附加信息的字段
    private String note;

    private String createBy;
    private Date createDate;
    private String lastUpdateBy;
    private Date lastUpdateDate;

}
