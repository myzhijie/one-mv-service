package com.jingsky.mv.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * 用来获取表的前缀
 */
@Configuration
@Slf4j
public class TablePrefixConfig {
    //maxwell用到的表前缀
    private static String tablePrefix;

    public static String getTablePrefix(){
        return tablePrefix;
    }

    @Value("${maxwell.tablePrefix}")
    public static void setTablePrefix(String tablePrefix){
        TablePrefixConfig.tablePrefix=tablePrefix;
    }


    public static Integer getClientId(){
        return sumStrAscii(getTablePrefix());
    }

    /**
     * 字符串中字符Ascii数字和
     * @param str 字符串
     * @return int
     */
    private static int sumStrAscii(String str){
        byte[] byteStr = str.getBytes();
        int sum = 0;
        for(int i=0;i<byteStr.length;i++){
            sum += byteStr[i];
        }
        return sum;
    }
}

