package com.jingsky.mv.service;

import com.jingsky.mv.config.TablePrefixConfig;
import com.jingsky.mv.util.DatabaseService;
import com.jingsky.mv.vo.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
    //视图的主键名
    public static final String VIEW_PK="id";

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
            deleteById(TablePrefixConfig.getTablePrefix()+"well_bootstrap", map.get("id"));
        }

        //将不存在的表循环插入到bootstrap表中
        List<View> viewsList = getAllView();
        for (View view : viewsList) {
            //bootstrap表中已经存在的不插入
            Object bootstrapDb = findById("well_bootstrap", view.getId(), Object.class);
            if(bootstrapDb==null) {
                StringBuffer insertSb = new StringBuffer("insert into " + TablePrefixConfig.getTablePrefix() + "well_bootstrap");
                insertSb.append("(`id`,`table_name`,`repeat_order`,`client_id`,`custom_sql`,`batch_num`,`database_name`)values (?,?,?,?,?,?,?)");
                toDatabaseService.execute(insertSb.toString(), view.getId(), view.getMasterTable(), view.getId(), TablePrefixConfig.getClientId(), view.getSourceSql(), 1000, fromDatabaseService.getDatabase());
                //判断视图表是否存在，不存在创建视图表
                createViewIfNotExist(view);
            }
        }
    }

    /**
     * 如果视图表不存在，则创建视图表
     * @param view 视图
     */
    private void createViewIfNotExist(View view) throws Exception {
        String sql=makeCreateViewSql(view);
        log.info("Executing create view sql :\n"+sql);
        toDatabaseService.execute(sql);
    }

    /**
     * 生成创建视图表SQL
     * @param view 视图
     * @return String
     */
    public String makeCreateViewSql(View view) throws Exception {
        //列中的字段收集，源表+源字段：视图中字段名
        Map<String,String> colMap=new HashMap<>();
        StringBuffer sb=new StringBuffer("CREATE TABLE IF NOT EXISTS `"+view.getMvName()+"` ( \n");
        //首先生成id
        ColumnInfo columnInfo=fromDatabaseService.getColumnInfo(view.getMasterTable(),view.getMasterTablePk());
        sb.append("    `"+ConfigService.VIEW_PK+"` "+columnInfo.getType()+" NOT NULL COMMENT '"+view.getMasterTable()+":"+view.getMasterTablePk()+",不可修改',\n");
        //拼接列
        for(ViewCol col : view.getViewColList()){
            //主键不再重复添加
            if(col.getSourceTable().equals(view.getMasterTable()) && col.getSourceCol().equals(view.getMasterTablePk())){
                continue;
            }
            columnInfo=fromDatabaseService.getColumnInfo(col.getSourceTable(),col.getSourceCol());
            sb.append("    `"+col.getCol()+"` "+columnInfo.getType()+" DEFAULT NULL COMMENT '"+columnInfo.getComment()+"',\n");
            colMap.put(col.getSourceTable()+"_"+col.getSourceCol(),col.getCol());
        }
        //拼接left join
        for(ViewLeftJoin leftJoin : view.getViewLeftJoinList()){
            String tableColName=leftJoin.getTable()+"_"+leftJoin.getJoinCol();
            //已经存在的列不需要增加
            if(colMap.keySet().contains(tableColName)) {
                sb.append("    KEY `key_"+colMap.get(tableColName)+"` (`"+colMap.get(tableColName)+"`),\n");
            }else{
                columnInfo=fromDatabaseService.getColumnInfo(leftJoin.getTable(),leftJoin.getJoinCol());
                sb.append("    `" +tableColName+ "` "+columnInfo.getType());
                sb.append(" DEFAULT NULL COMMENT '"+columnInfo.getComment()+"',\n");
                sb.append("    KEY `key_"+tableColName+"` (`"+tableColName+"`),\n");
            }
        }
        sb.append("`_updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '无用，仅参考',\n");
        sb.append("    PRIMARY KEY (`"+ConfigService.VIEW_PK+"`)\n");
        sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
        return sb.toString();
    }

    /**
     * 获取所有视图
     *
     * @return List<View>
     */
    public List<View> getAllView() throws SQLException, URISyntaxException {
        List<View> viewList = new ArrayList<>();
        List<TableView> tableViewList = findAllView();
        for (TableView tableView : tableViewList) {
            viewList.add(new View(tableView));
        }
        return viewList;
    }

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
     * 获取所有视图配置
     *
     * @return
     * @throws SQLException
     * @throws URISyntaxException
     */
    public List<TableView> findAllView() throws SQLException, URISyntaxException {
        List<TableView> tableViewList = toDatabaseService.query("select * from " + TablePrefixConfig.getTablePrefix()+"table_view", TableView.class);
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
