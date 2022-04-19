package com.jingsky.mv.util.exception;

import com.jingsky.mv.maxwell.row.RowMap;
import lombok.Data;

/**
 * 全量时的异常类
 */
@Data
public class BootstrapException extends Exception{
    private RowMap rowMap;

    public BootstrapException(RowMap rowMap, Exception exception){
        super(exception);
        this.rowMap=rowMap;
    }
}
