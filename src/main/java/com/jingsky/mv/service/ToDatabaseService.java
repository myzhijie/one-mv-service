package com.jingsky.mv.service;

import com.jingsky.mv.util.IDatasourceConfig;
import com.jingsky.mv.util.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * 目标数据库操作Service
 */
@Service
@Slf4j
public class ToDatabaseService extends DatabaseService {
    @Autowired
    @Qualifier("toDatasourceConfig")
    private IDatasourceConfig datasourceConfig;

    @Override
    public IDatasourceConfig getDatasourceConfig() {
        return datasourceConfig;
    }
}
