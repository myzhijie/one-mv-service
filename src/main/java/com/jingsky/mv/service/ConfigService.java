package com.jingsky.mv.service;

import com.jingsky.mv.config.TablePrefixConfig;
import com.jingsky.mv.util.AsciiUtil;
import com.jingsky.mv.util.DatabaseService;
import com.jingsky.mv.vo.TableView;
import com.jingsky.mv.vo.View;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 操作配置的service
 */
@Service
@Slf4j
public class ConfigService {
    @Autowired
    @Qualifier("toDatabaseService")
    private DatabaseService toDatabaseService;
    @Autowired
    private FromDatabaseService fromDatabaseService;
    //mysql client ID
    private String clientId = AsciiUtil.sumStrAscii(TablePrefixConfig.getTablePrefix()) + "";
    //所有视图列表
    private List<View> viewList;

    /**
     * 将需要进行全量迁移的表插入到bootstrap表中
     */
    public void insertBootstrapTable() throws Exception {
        List<Map<String, Object>> bootstrapMapList = findStartAndUnCompleteBootstrap();
        for (Map<String, Object> map : bootstrapMapList) {
            //删除对应视图中的数据
            TableView tableView = findById("table_view", map.get("id"), TableView.class);
            toDatabaseService.execute("delete from " + tableView.getMvName());
            //从bootstrap表中删除此数据
            deleteById(getRealTableName("well_bootstrap"), map.get("id"));
        }

        //将不存在的表循环插入到bootstrap表中
        List<View> viewsList = getAllView();
        for (View view : viewsList) {
            StringBuffer insertSb = new StringBuffer("insert into " + getRealTableName("well_bootstrap"));
            insertSb.append("(`id`,`table_name`,`repeat_order`,`client_id`,`custom_sql`,`batch_num`,`database_name`)values (?,?,?,?,?,?,?)");
            toDatabaseService.execute(insertSb.toString(), view.getId(), view.getMasterTable(), view.getId(), clientId, view.toSql(), 1000, fromDatabaseService.getDatabase());
        }
    }

    /**
     * 获取所有视图
     *
     * @return
     */
    public List<View> getAllView() throws SQLException, URISyntaxException {
        if (CollectionUtils.isEmpty(this.viewList)) {
            this.viewList = new ArrayList<>();
            List<TableView> tableViewList = findAllView();
            for (TableView tableView : tableViewList) {
                this.viewList.add(new View(tableView));
            }
        }
        return this.viewList;
    }

    /**
     * 获取启动但是未bootstrap完成的任务
     *
     * @return List<Map < String, Object>>
     * @throws SQLException
     * @throws URISyntaxException
     */
    public List<Map<String, Object>> findStartAndUnCompleteBootstrap() throws SQLException, URISyntaxException {
        String sql = "select * from " + getRealTableName("well_bootstrap") + " where is_complete=0 and started_at is not null";
        return toDatabaseService.query(sql);
    }

    /**
     * 获取所有视图配置
     *
     * @return
     * @throws SQLException
     * @throws URISyntaxException
     */
    public List<TableView> findAllView() throws SQLException, URISyntaxException {
        List<TableView> tableViewList = toDatabaseService.query("select * from " + getRealTableName("table_view"), TableView.class);
        return tableViewList;
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
        String sql = "select * from " + getRealTableName(tableName) + " where id=? ";
        return toDatabaseService.queryOne(sql, cls, id);
    }

    /**
     * 根据id删除某表中的数据
     *
     * @param tableName 表名
     * @param id        id
     */
    public int deleteById(String tableName, Object id) throws Exception {
        String sql = "delete from " + getRealTableName(tableName) + " where id=? ";
        int num = toDatabaseService.execute(sql, new Object[]{id});
        return num;
    }

    /**
     * 增加前缀获取真实表名
     *
     * @param tableName
     * @return
     */
    private String getRealTableName(String tableName) {
        String prefix = TablePrefixConfig.getTablePrefix();
        return "t_" + prefix + "_" + tableName;
    }
}
