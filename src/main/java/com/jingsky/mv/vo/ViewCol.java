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

    public ViewCol(String col,String sourceTable,String sourceCol){
        this.col=col;
        this.sourceTable=sourceTable;
        this.sourceCol=sourceCol;
    }
}
