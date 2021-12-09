package com.jingsky.mv.service;

import com.jingsky.mv.util.DatabaseService;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * 源数据库操作Service
 */
@Service
@Slf4j
public class FromDatabaseService extends DatabaseService {
    @Autowired
    private HikariDataSource fromDataSource;

    @Override
    public HikariDataSource getDatasource() {
        return fromDataSource;
    }
}
