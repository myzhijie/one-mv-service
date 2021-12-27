package com.jingsky.mv.vo;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 视图类列映射,非数据表
 */
@Data
@NoArgsConstructor
public class ViewCol {
    /**
     * 视图中的列名
     */
    private String col;
    /**
     * 源表名
     */
    private String sourceTable;
    /**
     * 源列名
     */
    private String sourceCol;
    /**
     * 聚合函数，只能avg,count,min,max,sum
     */
    private String aggregateFunction;
}
