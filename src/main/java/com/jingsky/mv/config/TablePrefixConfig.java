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
    @Autowired
    @Qualifier("toDatasourceConfig")
    private IDatasourceConfig datasourceConfig;
    //表的前缀
    private static String tablePrefix;

    /**
     * 仅仅为了初始化tablePrefix
     * @return String
     */
    @Bean(name = "prefixStr")
    public String prefixStr() throws Exception {
        setTablePrefix();//设置下表名前缀
        return getTablePrefix();
    }

    /**
     * 在这里计算出tablePrefix
     * @return String
     */
    private void setTablePrefix(){
        TablePrefixConfig.tablePrefix="t_"+ datasourceConfig.getPrefix()+"_";
    }

    public static String getTablePrefix(){
        return tablePrefix;
    }
}

