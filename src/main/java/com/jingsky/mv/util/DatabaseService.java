package com.jingsky.mv.util;

import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 数据库服务基础抽象类
 */
@Slf4j
@Getter
public class DatabaseService {
    private HikariDataSource dataSource;
    private String username;
    private String password;
    private String host;
    private Integer port;
    private String database;

    public DatabaseService(HikariDataSource dataSource){
        this.dataSource=dataSource;
        URI uri = URI.create(dataSource.getJdbcUrl().substring(5));
        this.host=uri.getHost();
        this.port=uri.getPort();
        this.database=uri.getPath().substring(1);
        this.username=dataSource.getUsername();
        this.password=dataSource.getPassword();
    }

    /**
     * 执行sql语句
     * @param sql 被执行的sql语句
     * @return 受影响的行
     * @throws Exception
     */
    public int execute(String sql) throws Exception {
        Connection conn = dataSource.getConnection();
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
        Connection conn = dataSource.getConnection();
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
            conn = dataSource.getConnection();
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
            conn = dataSource.getConnection();
            QueryRunner qr = new QueryRunner();
            results = qr.query(conn, sql, new MapListHandler(), param);
        } finally {
            conn.close();
        }
        return results;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
