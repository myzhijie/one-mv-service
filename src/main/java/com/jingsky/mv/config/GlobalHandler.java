package com.jingsky.mv.config;

import com.jingsky.mv.util.exception.BootstrapException;
import com.jingsky.mv.util.exception.IncrementException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 全局处理handler
 */
@Service
@Slf4j
public class GlobalHandler {

    /**
     * 当执行全量异常时进行回调
     */
    public void handleBootstrapException(BootstrapException exception) {
    }

    /**
     * 当执行增量异常时进行回调
     */
    public void handleIncrementException(IncrementException exception) {
    }

    /**
     * 有异常，但非全量和增量异常时进行回调
     */
    public void handleOtherException(Exception exception) {
    }
}
