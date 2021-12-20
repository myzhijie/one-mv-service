package com.jingsky.mv.maxwell.producer;

import com.jingsky.mv.maxwell.row.RowMap;
import com.jingsky.mv.vo.ColumnInfo;
import com.jingsky.mv.vo.View;
import com.jingsky.mv.vo.ViewCol;
import com.jingsky.mv.vo.ViewLeftJoin;
import com.jingsky.mv.service.ConfigService;
import com.jingsky.mv.util.DatabaseService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * FileProducer 辅助类,主要用于对View进行操作。
 */
@Slf4j
@Service
public class ViewProducerHelper {
    //目标数据源服务
    @Autowired
    private DatabaseService toDatabaseService;
    //源数据源服务
    @Autowired
    private DatabaseService fromDatabaseService;
    //视图在数据表中配置的Dao
    @Autowired
    private ConfigService configService;
    //需要进行处理的全量表
    private Map<String, View> bootstrapTableMap = new HashMap<>();
    //表和涉及到视图的对应
    private Map<String,List<View>> tableViewsMap=new HashMap<>();
    //表视图ID和列的对应
    private Map<String,List<ViewCol>> tableViewColsMap=new HashMap<>();
    //表视图ID和更新时索引字段和老数据表中字段的对应
    private Map<String,String[]> tableViewUpdateIdMap=new HashMap<>();


    /**
     * 批量插入到数据表
     * @param table 表名
     * @param rowMapList
     */
    public void handleBootstrapInsert(String table, List<RowMap> rowMapList) throws Exception {
        List<Map<String, Object>> dataMapList=new ArrayList<>();
        for(RowMap rowMap:rowMapList){
            dataMapList.add(rowMap.getData());
        }
        String sql=makeInsertSql(table,dataMapList);
        toDatabaseService.execute(sql);
    }

    /**
     * 更新视图中的值
     * @param view 视图
     * @param rowMap 行数据Map
     */
    public void updateData4View(View view,RowMap rowMap) throws Exception {
        //视图中的where字段
        String whereCol=getTableViewUpdateId(rowMap.getTable(),view.getId());
        //源表中的where字段
        String whereColSource=getTableViewUpdateSourceCol(rowMap.getTable(),view.getId());

        List<ViewCol> colList=getViewColsByTable(rowMap.getTable(),view.getId());
        StringBuffer updateSql=new StringBuffer("update "+view.getMvName()+" set ");
        for(ViewCol viewCol : colList){
            if(!whereCol.equals(viewCol.getCol())){
                updateSql.append(viewCol.getCol()+"='"+rowMap.getData(viewCol.getSourceCol())+"',");
            }
        }
        String sql=updateSql.substring(0,updateSql.length()-1)+" where "+whereCol+"='"+rowMap.getData(whereColSource)+"'";
        int num=toDatabaseService.execute(sql);
        log.info("update sql:"+sql);
        if(view.getMasterTable().equals(rowMap.getTable()) && num<1){
            throw new RuntimeException("SQL result must >=1,but now:"+num+",sql:"+sql);
        }
    }

    /**
     * 删除视图中的行
     * @param rowMap 行数据Map
     * @param view 视图
     * @throws Exception
     */
    public void delData4View(RowMap rowMap, View view) throws Exception {
        String delSql="delete from "+view.getMvName()+" where id=?";
        int num=toDatabaseService.execute(delSql,rowMap.getData(view.getMasterTablePk()));
        log.info("delete sql:"+delSql.substring(0,delSql.length()-1)+rowMap.getData(view.getMasterTablePk()));
        if(num!=1){
            throw new RuntimeException("SQL result must 1,but now:"+num+",sql:"+delSql);
        }
    }

    /**
     * 向view中插入数据
     * @param rowMap
     * @param view
     * @throws Exception
     */
    public void insertData4View(RowMap rowMap, View view) throws Exception {
        String querySql=view.tmpAddIdToMasterWhere(rowMap.getData().get(view.getMasterTablePk()).toString());
        List<Map<String, Object>> dataList=fromDatabaseService.query(querySql);
        if(dataList!=null && dataList.size()>1){
            throw new RuntimeException("SQL result must 1,but now:"+dataList.size()+",sql:"+querySql);
        }
        String sql=makeInsertSql(view.getMvName(),dataList);
        log.info("insert sql:"+sql);
        toDatabaseService.execute(sql);
    }

    /**
     * 检查行数据是否在where条件下
     * @param table 表名
     * @param whereSql where条件
     * @param dataRowMap 数据行
     * @return boolean true 在
     * @throws SQLException
     */
    public boolean chkDateExistInWhere(String table,String whereSql,Map<String, Object> dataRowMap) throws Exception {
        if(StringUtils.isBlank(whereSql)){
            return true;
        }
        //创建临时表
        String tmpTableSub= UUID.randomUUID().toString().replace("-","");
        String createSql="create temporary table "+table+tmpTableSub+" like "+table;
        fromDatabaseService.execute(createSql);
        //插入数据
        List<Map<String,Object>> list=new ArrayList<>();
        list.add(dataRowMap);
        fromDatabaseService.execute(makeInsertSql(table+tmpTableSub,list));
        //查询SQL看是否可以匹配
        String sql="select * from "+table+tmpTableSub+" where "+whereSql;
        List<Map<String, Object>> results = fromDatabaseService.query(sql);
        return CollectionUtils.isNotEmpty(results);
    }

    /**
     * 批量插入到数据表
     * @param table 表名
     * @param dataMapList
     */
    public String makeInsertSql(String table,List<Map<String, Object>> dataMapList) {
        StringBuffer sb=new StringBuffer("insert into "+table+"(");
        //读取列表名
        for(String colName : dataMapList.get(0).keySet()){
            sb.append("`"+colName+"`,");
        }
        sb.append(") values ");
        //写入值
        for(Map<String, Object> map : dataMapList){
            sb.append("(");
            for(String colName : map.keySet()){
                sb.append("'" + map.get(colName) + "',");
            }
            sb.append("),");
        }
        //去掉一些特殊字符 最后字段的,和语句最后的,
        String sql=sb.toString().replaceAll(",\\)",")");
        sql=sql.replaceAll(",$","");
        return sql;
    }

    /**
     * 初始化表视图列的映射关系
     * @param table 表
     * @param view 视图
     */
    private void initTableViewColMap(String table, View view) {
        List<ViewCol> viewColList=tableViewColsMap.get(table+view.getId());
        if(viewColList==null){
            viewColList=new ArrayList<>();
        }
        List<ViewCol> colList=view.getViewColList();
        for(ViewCol col : colList){
            if(col.getSourceTable().equals(table)){
                viewColList.add(col);
            }
        }
        tableViewColsMap.put(table+view.getId(),viewColList);
    }

    /**
     * 生成BootstrapTableMap key
     *
     * @param tableName   表名
     * @param repeatOrder 重复顺序
     * @return String
     */
    public String generateBootstrapMapKey(String tableName, Integer repeatOrder) {
        return tableName + "_" + repeatOrder;
    }

    /**
     * 判断表名是否在所有视图中存在
     * @param table 表名
     * @return 存在 true
     */
    public boolean chkTableInView(String table) {
        return tableViewsMap.keySet().contains(table);
    }

    /**
     * 获取正在bootstrap的View
     * @param table 表名
     * @param repeatOrder 重复序号
     * @return View
     */
    public View getView4Bootstrap(String table, Integer repeatOrder) {
        return bootstrapTableMap.get(generateBootstrapMapKey(table,repeatOrder));
    }

    /**
     * 获取这个表关联的所有视图
     * @param table 表名
     * @return List<View>
     */
    public List<View> getViewsByTable(String table) {
        return tableViewsMap.get(table);
    }

    /**
     * 获取View下某表对应的所有列
     * @param table 表名
     * @param viewId 视图ID
     * @return List<ViewCol>
     */
    public List<ViewCol> getViewColsByTable(String table, Integer viewId) {
        return tableViewColsMap.get(table+viewId);
    }

    /**
     * 初始化关于视图的数据
     */
    public void initViewsData() throws SQLException, URISyntaxException {
        //先清理这里共享变量
        bootstrapTableMap = new HashMap<>();
        //表和涉及到视图的对应
        tableViewsMap=new HashMap<>();
        //表视图ID和列的对应
        tableViewColsMap=new HashMap<>();
        //表视图ID和更新时索引字段的对应
        tableViewUpdateIdMap=new HashMap<>();
        //初始化所有视图
        List<View> viewList=configService.getAllView();
        for(View view : viewList) {
            initViewData(view);
        }
    }

    /**
     * 初始化表更新时的索引字段
     * @param view
     */
    private void initTableViewUpdateIdMap(View view){
        //列中的字段收集，源表+源字段：视图中字段名
        Map<String,String> colMap=new HashMap<>();
        for(ViewCol col : view.getViewColList()){
            colMap.put(col.getSourceTable()+"_"+col.getSourceCol(),col.getCol());
        }
        for(ViewLeftJoin leftJoin : view.getViewLeftJoinList()){
            String tableColName=leftJoin.getTable()+"_"+leftJoin.getJoinCol();
            if(colMap.keySet().contains(tableColName)) {
                this.tableViewUpdateIdMap.put(leftJoin.getTable()+view.getId(),new String[]{colMap.get(tableColName),leftJoin.getJoinCol()});
            }else{
                this.tableViewUpdateIdMap.put(leftJoin.getTable()+view.getId(),new String[]{tableColName,leftJoin.getJoinCol()});
            }
        }
        //记录master表主键
        this.tableViewUpdateIdMap.put(view.getMasterTable()+view.getId(),new String[]{ConfigService.VIEW_PK,view.getMasterTablePk()});
    }

    /**
     * 初始化一些此View的数据
     * @param view
     */
    private void initViewData(View view) {
        bootstrapTableMap.put(generateBootstrapMapKey(view.getMasterTable(), view.getId()), view);
        //放入表和视图的对应关系
        List<View> viewListTmp=tableViewsMap.get(view.getMasterTable());
        if(viewListTmp==null){
            viewListTmp=new ArrayList<>();
        }
        viewListTmp.add(view);
        tableViewsMap.put(view.getMasterTable(),viewListTmp);
        for(ViewLeftJoin viewLeftJoin : view.getViewLeftJoinList()){
            List<View> viewListJoin=tableViewsMap.get(viewLeftJoin.getTable());
            if(viewListJoin==null){
                viewListJoin=new ArrayList<>();
            }
            viewListJoin.add(view);
            tableViewsMap.put(viewLeftJoin.getTable(),viewListJoin);
            initTableViewColMap(viewLeftJoin.getTable(),view);
        }
        initTableViewColMap(view.getMasterTable(),view);
        initTableViewUpdateIdMap(view);
        log.info("Add view:" + view.getMvName());
    }

    /**
     * 根据表名和视图id获取更新时的索引字段
     * @param tableName 表名
     * @param viewId 视图id
     * @return String
     */
    public String getTableViewUpdateId(String tableName,Integer viewId){
        return tableViewUpdateIdMap.get(tableName+viewId)[0];
    }

    /**
     * 根据表名和视图id获取更新时的索引字段在原始表中的字段名
     * @param tableName 表名
     * @param viewId 视图id
     * @return String
     */
    public String getTableViewUpdateSourceCol(String tableName,Integer viewId){
        return tableViewUpdateIdMap.get(tableName+viewId)[1];
    }
}
