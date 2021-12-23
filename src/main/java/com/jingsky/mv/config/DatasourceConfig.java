package com.jingsky.mv.config;

import com.jingsky.mv.util.DatabaseService;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "spring.hikari")
@Data
public class DatasourceConfig {
    private HikariDataSource from;
    private HikariDataSource to;

    @Bean("fromDatabaseService")
    public DatabaseService fromDatabaseService() {
        return new DatabaseService(from);
    }

    @Bean("toDatabaseService")
    public DatabaseService toDatabaseService() {
        return new DatabaseService(to);
    }
}