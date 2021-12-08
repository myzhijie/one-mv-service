package com.jingsky.mv.mv;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 视图类中left join的表
 */
@Data
@NoArgsConstructor
public class ViewLeftJoin {
    //表名
    private String table;
    //join时左表的字段
    private String joinLeftCol;
    //join时当前表的字段
    private String joinCol;

    public ViewLeftJoin(String table,String joinLeftCol,String joinCol){
        this.table=table;
        this.joinLeftCol=joinLeftCol;
        this.joinCol=joinCol;
    }
}
