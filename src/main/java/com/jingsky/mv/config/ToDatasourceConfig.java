package com.jingsky.mv.config;

import com.jingsky.mv.util.IDatasourceConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * 项目使用的数据库信息配置
 */
@Data
@Slf4j
@Component("toDatasourceConfig")
public class ToDatasourceConfig implements IDatasourceConfig {
    //项目使用数据源表名前缀，不能在config上配置，需要在启动时在环境变量上传入。
    @Value("${prefix:view}")
    private String prefix;
    @Value("${spring.datasource.druid.board.url}")
    private String url;
    @Value("${spring.datasource.druid.board.username}")
    private String username;
    @Value("${spring.datasource.druid.board.password}")
    private String password;
    //以下从url中获取
    private String host;
    private Integer port;
    private String database;

    @Bean(name = "toDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.druid.board")
    public DataSource dataSource() {
        if("0".equals(prefix)){
            log.error("您需要在环境变量中配置下表前缀prefix=值。(You need config table prefix with variable prefix=value in environment variables.)");
            System.exit(0);
        }
        return DataSourceBuilder.create().type(com.alibaba.druid.pool.DruidDataSource.class).build();
    }

    /**
     * 链接端口，给maxwell使用
     *
     * @return Integer
     */
    public Integer getPort() {
        if (port == null) {
            //从URL中提取port
            String tmp = url.substring(url.lastIndexOf(":") + 1);
            tmp = tmp.substring(0, tmp.indexOf("/"));
            port = Integer.parseInt(tmp);
        }
        return port;
    }

    /**
     * 链接Host，给maxwell使用
     *
     * @return String
     */
    public String getHost() {
        if (host == null) {
            //从URL中提取host
            host = url.substring(url.lastIndexOf("//") + 2);
            host = host.substring(0, host.indexOf(":"));
        }
        return host;
    }

    /**
     * 链接数据库名，给maxwell使用
     *
     * @return String
     */
    public String getDatabase() {
        if (database == null) {
            //从URL中提取database
            database = url.substring(url.lastIndexOf("/") + 1);
            database = database.substring(0, database.indexOf("?"));
        }
        return database;
    }
}

