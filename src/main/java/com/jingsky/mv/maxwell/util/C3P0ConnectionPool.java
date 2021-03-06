package com.jingsky.mv.maxwell.util;

import com.jingsky.mv.maxwell.errors.DuplicateProcessException;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.NoSuchElementException;

@Slf4j
public class C3P0ConnectionPool implements ConnectionPool {
	private final ComboPooledDataSource cpds;
	static final Logger LOGGER = LoggerFactory.getLogger(C3P0ConnectionPool.class);

	@Override
	public Connection getConnection() throws SQLException {
		//log.info("总："+cpds.getNumConnectionsDefaultUser()+",忙："+cpds.getNumBusyConnectionsDefaultUser()+",空闲："+cpds.getNumIdleConnectionsDefaultUser()+",未关闭:"+cpds.getNumUnclosedOrphanedConnectionsAllUsers());
		return cpds.getConnection();
	}

	@Override
	public void release() {
		cpds.close();
	}

	public C3P0ConnectionPool(String url, String user, String password) {
		cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(url);
		cpds.setUser(user);
		cpds.setPassword(password);
		cpds.setTestConnectionOnCheckout(true);

		// the settings below are optional -- c3p0 can work with defaults
		cpds.setMinPoolSize(3);
		cpds.setMaxPoolSize(50);
	}

	public void probe() throws SQLException {
		cpds.setAcquireRetryAttempts(1);
		try ( Connection c = getConnection() ) {
			cpds.setAcquireRetryAttempts(30);
		} catch ( SQLException e ) {
			// the sql exception thrown here is worthless, it's just
			// "coudln't get connection from pool."  dig out the goods.
			Throwable t = cpds.getLastAcquisitionFailureDefaultUser();
			if ( t instanceof SQLException ) {
				throw((SQLException) t);
			} else {
				throw new RuntimeException("couldn't get connection from pool", t);
			}
		}
	}

	@Override
	public void withSQLRetry(int nTries, RetryableSQLFunction<Connection> inner)
		throws SQLException, DuplicateProcessException, NoSuchElementException {
		try ( final Connection c = getConnection() ){
			inner.apply(c);
			return;
		} catch (SQLException e) {
			if ( nTries > 0 ) {
				LOGGER.error("got SQL Exception: {}, retrying...",
					e.getLocalizedMessage()
				);
				withSQLRetry(nTries - 1, inner);
			} else {
				throw(e);
			}
		}
	}
}
