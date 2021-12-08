package com.jingsky.mv.config;

import com.jingsky.mv.util.IDatasourceConfig;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * 进行数据传输的源数据源配置
 */
@Data
@ConfigurationProperties("spring.datasource.from")
@Component("fromDatasourceConfig")
public class FromDatasourceConfig implements IDatasourceConfig {
    private String host;
    private Integer port;
    private String database;
    private String username;
    private String password;

    @Override
    @Bean(name = "fromDataSource")
    public DataSource dataSource() {
        return DataSourceBuilder.create().type(com.alibaba.druid.pool.DruidDataSource.class).build();
    }
}

