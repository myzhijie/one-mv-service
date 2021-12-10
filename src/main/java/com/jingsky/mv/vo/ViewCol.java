package com.jingsky.mv.vo;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 视图类列映射,非数据表
 */
@Data
@NoArgsConstructor
public class ViewCol {

    public ViewCol(String col,String sourceTable,String sourceCol){
        this.col=col;
        this.sourceTable=sourceTable;
        this.sourceCol=sourceCol;
    }

    //列名
    private String col;
    //源表
    private String sourceTable;
    //源列
    private String sourceCol;
}
