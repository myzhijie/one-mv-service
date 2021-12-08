package com.jingsky.mv.util.exception;

import com.jingsky.mv.maxwell.row.RowMap;
import lombok.Data;

/**
 * 增量时的异常类
 */
@Data
public class IncrementException extends Exception{
    private RowMap rowMap;

    public IncrementException(RowMap rowMap,Exception exception){
        super(exception);
        this.rowMap=rowMap;
    }

}
