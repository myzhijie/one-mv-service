package com.jingsky.mv.util.exception;

import com.jingsky.mv.maxwell.row.RowMap;
import com.jingsky.mv.entity.View;
import lombok.Data;

/**
 * 全量时的异常类
 */
@Data
public class BootstrapException extends Exception{
    private View view;
    private RowMap rowMap;

    public BootstrapException(View view, RowMap rowMap, Exception exception){
        super(exception);
        this.view = view;
        this.rowMap=rowMap;
    }
}
