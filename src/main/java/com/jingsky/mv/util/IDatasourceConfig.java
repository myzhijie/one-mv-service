package com.jingsky.mv.util;

import javax.sql.DataSource;

/**
 * 数据源配置接口
 */
public interface IDatasourceConfig {

    DataSource dataSource();

    /**
     * 数据库端口
     *
     * @return Integer
     */
    Integer getPort() ;

    /**
     * 数据库Host
     *
     * @return String
     */
    String getHost();

    /**
     * 链接数据库名
     *
     * @return String
     */
    String getDatabase();

    /**
     * 用户名
     *
     * @return String
     */
    String getUsername();

    /**
     * 密码
     *
     * @return String
     */
    String getPassword();

    /**
     * 数据表前缀
     *
     * @return String
     */
    default String getPrefix(){
        return "";
    }
}

