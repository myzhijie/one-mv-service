package com.jingsky.mv.util;

import com.jingsky.mv.maxwell.MaxwellMysqlConfig;
import com.jingsky.mv.maxwell.util.C3P0ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 数据库服务基础抽象类
 */
@Slf4j
abstract public class DatabaseService {
    //database对应连接池Map
    private C3P0ConnectionPool connectionPool;

    public DatabaseService(){
    }

    abstract public IDatasourceConfig getDatasourceConfig();

    /**
     * 将数据源链接重置为空
     */
    public void releaseConnectionPool() {
        if (connectionPool != null) {
            connectionPool.release();
            connectionPool = null;
        }
    }

    /**
     * 获取源数据库连接池
     *
     * @return C3P0ConnectionPool
     */
    private C3P0ConnectionPool getConnectionPool() throws SQLException, URISyntaxException {
        if (connectionPool == null) {
            MaxwellMysqlConfig maxwellMysqlConfig = new MaxwellMysqlConfig(
                    getDatasourceConfig().getHost(),
                    getDatasourceConfig().getPort(),
                    getDatasourceConfig().getDatabase(),
                    getDatasourceConfig().getUsername(),
                    getDatasourceConfig().getPassword(),
                    null
            );
            connectionPool = new C3P0ConnectionPool(maxwellMysqlConfig.getConnectionURI(true), getDatasourceConfig().getUsername(), getDatasourceConfig().getPassword());
            connectionPool.probe();
        }
        return connectionPool;
    }

    /**
     * 获取一个链接
     * @return
     * @throws Exception
     */
    public Connection getConnection() throws Exception {
        return getConnectionPool().getConnection();
    }

    /**
     * 执行sql语句
     * @param sql 被执行的sql语句
     * @return 受影响的行
     * @throws Exception
     */
    public int execute(String sql) throws Exception {
        Connection conn = getConnectionPool().getConnection();
        int rows = 0;
        try {
            QueryRunner qr = new QueryRunner();
            rows = qr.update(conn, sql);
        } finally {
            conn.close();
        }
        return rows;
    }

    /**
     * 执行含参数的sql语句
     * @param sql 被执行的sql语句
     * @param params 参数
     * @return 返回受影响的行
     * @throws Exception
     */
    public int execute(String sql, Object... params) throws Exception {
        Connection conn = getConnectionPool().getConnection();
        int rows = 0;
        try {
            QueryRunner qr = new QueryRunner();
            rows = qr.update(conn, sql, params);
        } finally {
            conn.close();
        }
        return rows;
    }

    /**
     * 查询sql语句且返回一个对象。
     *
     * @param sql 被执行的sql语句
     * @param cls 查询出来的bean类信息。
     * @return T
     * @throws SQLException
     */
    public <T> T queryOne(String sql, Class<T> cls, Object... param) throws SQLException, URISyntaxException {
        List<T> results = query(sql,cls,param);
        if(results!=null && results.size()>1){
            throw new SQLException("Expected one result (or null) to be returned by queryOne(), but found: "+results.size());
        }
        if(results==null || results.size()==0){
            return null;
        }else{
            return results.get(0);
        }
    }

    /**
     * 查询sql语句。
     *
     * @param sql 被执行的sql语句
     * @param cls 查询出来的bean类信息。
     * @return List<T>
     * @throws SQLException
     */
    public <T> List<T> query(String sql, Class<T> cls, Object... param) throws SQLException, URISyntaxException {
        List<T> results = null;
        Connection conn = null;
        try {
            conn = getConnectionPool().getConnection();
            QueryRunner qr = new QueryRunner();
            BeanListHandler<T> beanListHandler = new BeanListHandler(cls, new BasicRowProcessor(new GenerousBeanProcessor()));
            results = qr.query(conn, sql, beanListHandler, param);
        } finally {
            conn.close();
        }
        return results;
    }

    /**
     * 根据参数查询sql语句
     *
     * @param sql   sql语句
     * @param param 参数数组
     * @return List<Map < String, Object>>
     * @throws SQLException
     */
    public List<Map<String, Object>> query(String sql, Object... param) throws SQLException, URISyntaxException {
        List<Map<String, Object>> results = null;
        Connection conn = null;
        try {
            conn = getConnectionPool().getConnection();
            QueryRunner qr = new QueryRunner();
            results = qr.query(conn, sql, new MapListHandler(), param);
        } finally {
            conn.close();
        }
        return results;
    }

}
