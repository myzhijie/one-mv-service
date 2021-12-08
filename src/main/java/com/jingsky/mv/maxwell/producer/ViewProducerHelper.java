package com.jingsky.mv.maxwell.producer;

import com.jingsky.mv.maxwell.row.RowMap;
import com.jingsky.mv.mv.View;
import com.jingsky.mv.mv.ViewCol;
import com.jingsky.mv.mv.ViewLeftJoin;
import com.jingsky.mv.service.CommonService;
import com.jingsky.mv.service.ToDatabaseService;
import com.jingsky.mv.util.DatabaseService;
import com.jingsky.mv.util.GetBeanUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
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
    private DatabaseService toDatabaseService = (ToDatabaseService) GetBeanUtil.getContext().getBean("toDatabaseService");
    //源数据源服务
    private DatabaseService fromDatabaseService = (ToDatabaseService) GetBeanUtil.getContext().getBean("fromDatabaseService");
    //视图在数据表中配置的Dao
    private CommonService commonService = (CommonService) GetBeanUtil.getContext().getBean("commonService");
    //需要进行处理的全量表
    private Map<String, View> bootstrapTableMap = new HashMap<>();
    //表和涉及到视图的对应
    private Map<String,List<View>> tableViewsMap=new HashMap<>();
    //表视图ID和列的对应
    private Map<String,List<ViewCol>> tableViewColsMap=new HashMap<>();
    //表视图ID和更新时索引字段的对应
    private Map<String,String> tableViewUpdateIdMap=new HashMap<>();

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
     * @param viewId 视图ID
     * @param viewName 视图名称
     * @param whereCol 更新时where列
     * @param rowMap 行数据Map
     */
    public void updateData4View(Integer viewId,String viewName, String whereCol,RowMap rowMap) throws Exception {
        List<ViewCol> colList=getViewColsByTable(rowMap.getTable(),viewId);
        StringBuffer updateSql=new StringBuffer("update "+viewName+" set ");
        for(ViewCol viewCol : colList){
            if(!whereCol.equals(viewCol.getCol())){
                updateSql.append(viewCol.getCol()+"='"+rowMap.getData(viewCol.getSourceCol())+"',");
            }
        }
        updateSql.append("1=1 where "+whereCol+"='"+rowMap.getData(whereCol)+"'");
        int num=toDatabaseService.execute(updateSql.toString());
        if(num<1){
            throw new RuntimeException("SQL result must >=1,but now:"+num+",sql:"+updateSql);
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
        int num=toDatabaseService.execute(delSql,rowMap.getData("id"));
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
        String querySql=view.tmpAddIdToMasterWhere(rowMap.getData().get("id").toString());
        List<Map<String, Object>> dataList=fromDatabaseService.query(querySql);
        if(dataList!=null && dataList.size()>1){
            throw new RuntimeException("SQL result must 1,but now:"+dataList.size()+",sql:"+querySql);
        }
        String sql=makeInsertSql(view.getMvName(),dataList);
        toDatabaseService.execute(sql);
    }

    /**
     * 判断修改前后对表的影响
     * @param rowMap
     * @param whereSql where语句
     * @return 0 不影响 1 需新增记录 -1 需减少记录 2需更新数据
     * @throws Exception
     */
     public Integer chkWhere4RowMap(RowMap rowMap,String whereSql) throws Exception {
        if(StringUtils.isEmpty(whereSql)){
            return 2;
        }
        //手动开启事务
        Connection conn=fromDatabaseService.getConnection();
        conn.setAutoCommit(false);
        QueryRunner qr = new QueryRunner();
        //尝试创建临时表
        String tmpTableSub= UUID.randomUUID().toString().replace("-","");
        String createSql="create temporary table if not exists "+rowMap.getTable()+tmpTableSub+" like "+rowMap.getTable();
        qr.update(conn, createSql);
        //判断老数据是否符合where条件
        boolean oldExist=rowMap.getOldData().size()<=0 ? false : chkDateExistInWhere(rowMap.getTable(),whereSql,rowMap.getOldData(),conn,tmpTableSub);
        //判断新数据是否符合where条件
        boolean newExist=chkDateExistInWhere(rowMap.getTable(),whereSql,rowMap.getData(),conn,tmpTableSub);
        //事务回滚
        conn.rollback();
        conn.close();
        return oldExist ? (newExist ? 2 : -1) : (newExist ? 1 : 0);
    }

    /**
     * 检查行数据是否在where条件下
     * @param table 表名
     * @param whereSql where条件
     * @param dataRowMap 数据行
     * @param conn 数据库连接
     * @param tmpTableSub 临时表后缀
     * @return boolean true 在
     * @throws SQLException
     */
    private boolean chkDateExistInWhere(String table,String whereSql,LinkedHashMap<String, Object> dataRowMap,Connection conn,String tmpTableSub) throws SQLException {
        List<Map<String,Object>> list=new ArrayList<>();
        list.add(dataRowMap);
        QueryRunner qr = new QueryRunner();
        qr.update(conn, makeInsertSql(table,list));
        //查询SQL看是否可以匹配
        String sql="select * from "+table+tmpTableSub+" where ("+whereSql+") and id='"+dataRowMap.get("id")+"' limit 1";
        List<Map<String, Object>> results = qr.query(conn, sql, new MapListHandler());;
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
     * 初始化表视图和视图中此表数据更新时where列对应Map
     * @param table 表
     * @param view 视图
     * @param tableViewUpdateIdMap 关联Map
     */
    private void initTableViewUpdateIdMap(String table, View view, Map<String,String> tableViewUpdateIdMap) {
        if(table.equals(view.getMasterTable())) {
            tableViewUpdateIdMap.put(table+view.getId(),view.getMasterTablePk());
        }else{
            for(ViewLeftJoin leftJoin : view.getViewLeftJoinList()){
                if(table.equals(leftJoin.getTable())){
                    tableViewUpdateIdMap.put(table+view.getId(),leftJoin.getJoinCol());
                    break;
                }
            }
        }
    }

    /**
     * 初始化表视图列的映射关系
     * @param table 表
     * @param view 视图
     */
    private void initTableViewColMap(String table, View view,Map<String,List<ViewCol>> tableViewColsMap) {
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
        tableViewColsMap.put(table,viewColList);
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
     * 获取View下某表对应的更新ID
     * @param table 表名
     * @param viewId 视图ID
     * @return List<ViewCol>
     */
    public String getViewUpdateIdByTable(String table, Integer viewId) {
        return tableViewUpdateIdMap.get(table+viewId);
    }

    /**
     * 初始化关于视图的数据
     */
    public void initViewsData() throws SQLException, URISyntaxException {
        List<View> viewList=commonService.getAllView();
        for(View view : viewList) {
            initViewData(view);
        }
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
            initTableViewColMap(viewLeftJoin.getTable(),view,tableViewColsMap);
            initTableViewUpdateIdMap(viewLeftJoin.getTable(),view,tableViewUpdateIdMap);
        }
        initTableViewColMap(view.getMasterTable(),view,tableViewColsMap);
        initTableViewUpdateIdMap(view.getMasterTable(),view,tableViewUpdateIdMap);
        log.info("Add view:" + view.getMvName());
    }
}
