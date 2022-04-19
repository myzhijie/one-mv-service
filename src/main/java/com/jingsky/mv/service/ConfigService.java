package com.jingsky.mv.service;

import com.jingsky.mv.config.TablePrefixConfig;
import com.jingsky.mv.util.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 操作配置的service
 */
@Service
@Slf4j
public class ConfigService {
    @Autowired
    private DatabaseService toDatabaseService;
    @Autowired
    private DatabaseService fromDatabaseService;

    /**
     * 获取启动但是未bootstrap完成的任务
     *
     * @return List<Map < String, Object>>
     * @throws SQLException
     * @throws URISyntaxException
     */
    public List<Map<String, Object>> findStartAndUnCompleteBootstrap() throws SQLException, URISyntaxException {
        String sql = "select * from " + TablePrefixConfig.getTablePrefix()+"well_bootstrap where is_complete=0 and started_at is not null";
        return toDatabaseService.query(sql);
    }

    /**
     * 获取根据ID从某表中获取对象
     *
     * @param tableName 表名
     * @param id
     * @param cls       实体类
     * @param <T>
     * @return T
     */
    public <T> T findById(String tableName, Object id, Class<T> cls) throws SQLException, URISyntaxException {
        String sql = "select * from " + TablePrefixConfig.getTablePrefix()+tableName + " where id=? ";
        return toDatabaseService.queryOne(sql, cls, id);
    }

    /**
     * 根据id删除某表中的数据
     *
     * @param tableName 表名
     * @param id        id
     */
    public int deleteById(String tableName, Object id) throws Exception {
        String sql = "delete from " + TablePrefixConfig.getTablePrefix()+tableName + " where id=? ";
        int num = toDatabaseService.execute(sql, new Object[]{id});
        return num;
    }
}
