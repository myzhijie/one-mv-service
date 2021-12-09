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
        return "mv";
    }
}

