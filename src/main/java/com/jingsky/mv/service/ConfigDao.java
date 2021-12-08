package com.jingsky.mv.service;

import com.jingsky.mv.config.TablePrefixConfig;
import com.jingsky.mv.entity.TableView;
import com.jingsky.mv.entity.WellBootstrap;
import com.jingsky.mv.util.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;

/**
 * 配置所在数据库使用的DAO
 */
@Service
@Slf4j
public class ConfigDao {
    @Autowired
    @Qualifier("toDatabaseService")
    private DatabaseService toDatabaseService;

    /**
     * 获取启动但是未bootstrap完成的任务
     * @return List<WellBootstrap>
     * @throws SQLException
     * @throws URISyntaxException
     */
    public List<WellBootstrap> findStartAndUnCompleteBootstrap() throws SQLException, URISyntaxException {
        String sql="select * from "+getRealTableName("bootstrap")+" where is_complete=0 and started_at is not null";
        List<WellBootstrap> list=toDatabaseService.query(sql, WellBootstrap.class);
        return list;
    }

    /**
     * 获取所有视图配置
     * @return
     * @throws SQLException
     * @throws URISyntaxException
     */
    public List<TableView> findAllView() throws SQLException, URISyntaxException {
        String prefix=toDatabaseService.getDatasourceConfig().getPrefix();
        List<TableView> tableViewList=toDatabaseService.query("select * from t_"+prefix+"_table_view", TableView.class);
        return tableViewList;
    }

    /**
     * 获取根据ID从某表中获取对象
     * @param tableName 表名
     * @param id
     * @param cls 实体类
     * @param <T>
     * @return T
     */
    public <T> T findById(String tableName,Object id,Class<T> cls) throws SQLException, URISyntaxException {
        String sql="select * from "+getRealTableName(tableName)+" where id=? ";
        return toDatabaseService.queryOne(sql,cls,id);
    }

    /**
     * 根据id删除某表中的数据
     * @param tableName 表名
     * @param id id
     */
    public int deleteById(String tableName, Object id) throws Exception {
        String sql="delete from "+getRealTableName(tableName)+" where id=? ";
        int num=toDatabaseService.execute(sql, new Object[]{id});
        return num;
    }

    /**
     * 增加前缀获取真实表名
     * @param tableName
     * @return
     */
    private String getRealTableName(String tableName) {
        String prefix = TablePrefixConfig.getTablePrefix();
        return "t_" + prefix + "_" + tableName;
    }
}
