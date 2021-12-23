package com.jingsky.mv.vo;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 视图类中left join的表,非数据表
 */
@Data
@NoArgsConstructor
public class ViewLeftJoin {
    /**
     * 被left join的表名
     */
    private String table;
    /**
     * join时主表中的字段
     */
    private String joinLeftCol;
    /**
     * 被left join的表中的字段
     */
    private String joinCol;

    public ViewLeftJoin(String table,String joinLeftCol,String joinCol){
        this.table=table;
        this.joinLeftCol=joinLeftCol;
        this.joinCol=joinCol;
    }
}
