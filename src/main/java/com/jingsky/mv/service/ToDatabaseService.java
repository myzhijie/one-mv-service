package com.jingsky.mv.service;

import com.jingsky.mv.util.DatabaseService;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 目标数据库操作Service
 */
@Service
@Slf4j
public class ToDatabaseService extends DatabaseService {
    @Autowired
    private HikariDataSource toDataSource;


    @Override
    public HikariDataSource getDatasource() {
        return toDataSource;
    }
}
