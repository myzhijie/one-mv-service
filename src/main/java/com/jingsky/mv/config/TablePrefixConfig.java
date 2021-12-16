package com.jingsky.mv.config;

import com.jingsky.mv.util.IDatasourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 用来获取表的前缀
 */
@Configuration
@Slf4j
public class TablePrefixConfig {

    public static String getTablePrefix(){
        return "t_mv_";
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

