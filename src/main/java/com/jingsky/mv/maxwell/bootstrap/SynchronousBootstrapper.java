package com.jingsky.mv.maxwell.bootstrap;

import com.jingsky.mv.config.TablePrefixConfig;
import com.jingsky.mv.maxwell.schema.Database;
import com.jingsky.mv.maxwell.schema.SchemaCapturer;
import com.jingsky.mv.maxwell.schema.columndef.*;
import com.jingsky.mv.maxwell.schema.columndef.ColumnDef;
import com.jingsky.mv.maxwell.schema.columndef.ColumnDefCastException;
import com.jingsky.mv.maxwell.schema.columndef.DateColumnDef;
import com.jingsky.mv.maxwell.schema.columndef.TimeColumnDef;
import com.jingsky.mv.maxwell.scripting.Scripting;
import com.jingsky.mv.maxwell.CaseSensitivity;
import com.jingsky.mv.maxwell.MaxwellMysqlStatus;
import com.jingsky.mv.maxwell.errors.DuplicateProcessException;
import com.jingsky.mv.maxwell.producer.MaxwellOutputConfig;
import com.jingsky.mv.maxwell.MaxwellContext;
import com.jingsky.mv.maxwell.producer.AbstractProducer;
import com.jingsky.mv.maxwell.row.RowMap;
import com.jingsky.mv.maxwell.schema.Schema;
import com.jingsky.mv.maxwell.schema.Table;
import com.jingsky.util.common.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SynchronousBootstrapper {
	class BootstrapAbortException extends Exception {
		public BootstrapAbortException(String message) {
			super(message);
		}
	}

	static final Logger LOGGER = LoggerFactory.getLogger(SynchronousBootstrapper.class);
	private static final long INSERTED_ROWS_UPDATE_PERIOD_MILLIS = 250;
	private final MaxwellContext context;

	private long lastInsertedRowsUpdateTimeMillis = 0;
	//private DateTimeColumnDef dateTimeColumnDef=new DateTimeColumnDef("custom","custom", (short) 0,1l);
	//private DateColumnDef dateColumnDef=new DateColumnDef("custom","custom", (short) 0);

	public SynchronousBootstrapper(MaxwellContext context) {
		this.context = context;
	}


	public void startBootstrap(BootstrapTask task, AbstractProducer producer, Long currentSchemaID) throws Exception {
		try {
			performBootstrap(task, producer, currentSchemaID);
		} catch ( BootstrapAbortException e ) {
			LOGGER.error("Bootstrap (id={}) aborted: {}", task.id, e.getMessage());
			setBootstrapRowToCompleted(0, task.id);
			throw e;
		}
		completeBootstrap(task, producer);
	}

	private Schema captureSchemaForBootstrap(BootstrapTask task) throws SQLException {
		try ( Connection cx = getConnection(task.database) ) {
			CaseSensitivity s = MaxwellMysqlStatus.captureCaseSensitivity(cx);
			SchemaCapturer c = new SchemaCapturer(cx, s, task.database, task.table);
			return c.capture();
		}
	}

	private Table getTableForTask(BootstrapTask task) throws BootstrapAbortException {
		Schema schema;
		try {
			schema = captureSchemaForBootstrap(task);
		} catch ( SQLException e ) {
			throw new BootstrapAbortException(e.getMessage());
		}

		Database database = schema.findDatabase(task.database);
		Table table = database.findTable(task.table);

		if ( table == null ) {
			String errMsg = String.format(
				"Couldn't find db/table for %s.%s",
				task.database, task.table
			);
			throw new BootstrapAbortException(errMsg);
		}
		return table;
	}

	public void performBootstrap(BootstrapTask task, AbstractProducer producer, Long currentSchemaID) throws Exception {
		LOGGER.debug("bootstrapping requested for " + task.logString());

		Table table = getTableForTask(task);

		producer.push(bootstrapStartRowMap(task, table));
		LOGGER.info(String.format("bootstrapping started for %s.%s", task.database, task.table));

		try ( Connection streamingConnection = getStreamingConnection(task.database)) {
			setBootstrapRowToStarted(task.id);
			//重新获取task内容
			task=getTaskById(task.id);
			ResultSet resultSet = getAllRows(task.customSql, streamingConnection);
			int insertedRows = 0;
			lastInsertedRowsUpdateTimeMillis = 0; // ensure updateInsertedRowsColumn is called at least once
			while ( resultSet.next() ) {
				RowMap row = bootstrapEventRowMap("bootstrap-insert", table.database, table.name, table.getPKList(), task.comment);
				//非自定义SQL时
				if(StringUtils.isEmpty(task.customSql)) {
					setRowValues(row, resultSet, table);
				}else{
					//自定义SQL时
					setRowValues4CustomSql(row, resultSet);
				}
				row.setSchemaId(currentSchemaID);

				Scripting scripting = context.getConfig().scripting;
				if ( scripting != null )
					scripting.invoke(row);
				//设置上表重复bootstrap时的序号
				row.setRepeatOrder(task.repeatOrder);
				row.setBatchNum(task.batchNum);
				//本次job唯一ID
				row.setJobId(task.jobId);
				if ( LOGGER.isDebugEnabled() )
					LOGGER.debug("bootstrapping row : " + row.toJSON());

				producer.push(row);
				Thread.sleep(1);

				//每一万条刷新数据库条数
				if(++insertedRows%10000==0){
					updateInsertedRowsColumn(insertedRows, task.id);
					LOGGER.info(String.format("bootstrapping for %s.%s,current inserted num of rows %s", task.database, task.table,insertedRows+""));
				}
			}
			setBootstrapRowToCompleted(insertedRows, task.id);
		} catch ( NoSuchElementException e ) {
			LOGGER.info("bootstrapping aborted for " + task.logString());
		}
	}

	private void updateInsertedRowsColumn(int insertedRows, Long id) throws SQLException, NoSuchElementException, DuplicateProcessException {
		this.context.getMaxwellConnectionPool().withSQLRetry(1, (connection) -> {
			long now = System.currentTimeMillis();
			if (now - lastInsertedRowsUpdateTimeMillis > INSERTED_ROWS_UPDATE_PERIOD_MILLIS) {
				String sql = "update `"+ TablePrefixConfig.getTablePrefix() +"well_bootstrap` set inserted_rows = ? where id = ?";
				PreparedStatement preparedStatement = connection.prepareStatement(sql);
				preparedStatement.setInt(1, insertedRows);
				preparedStatement.setLong(2, id);
				if (preparedStatement.executeUpdate() == 0) {
					throw new NoSuchElementException();
				}
				lastInsertedRowsUpdateTimeMillis = now;
			}
		});
	}

	protected Connection getConnection(String databaseName) throws SQLException {
		Connection conn = context.getReplicationConnection();
		conn.setCatalog(databaseName);
		return conn;
	}

	protected Connection getStreamingConnection(String databaseName) throws SQLException, URISyntaxException {
		Connection conn = DriverManager.getConnection(context.getConfig().replicationMysql.getConnectionURI(false), context.getConfig().replicationMysql.user, context.getConfig().replicationMysql.password);
		conn.setCatalog(databaseName);
		return conn;
	}

	private RowMap bootstrapStartRowMap(BootstrapTask task, Table table) {
		RowMap rowMap= bootstrapEventRowMap("bootstrap-start", table.database, table.name, table.getPKList(), task.comment);
		rowMap.setRepeatOrder(task.repeatOrder);
		rowMap.setBatchNum(task.batchNum);
		return rowMap;
	}

	private RowMap bootstrapEventRowMap(String type, String db, String tbl, List<String> pkList, String comment) {
		RowMap row = new RowMap(
			type,
			db,
			tbl,
			System.currentTimeMillis(),
			pkList,
			null);
		row.setComment(comment);
		return row;
	}

	public void completeBootstrap(BootstrapTask task, AbstractProducer producer) throws Exception {
		RowMap rowMap=bootstrapEventRowMap("bootstrap-complete", task.database, task.table, new ArrayList<>(), task.comment);
		rowMap.setRepeatOrder(task.repeatOrder);
		rowMap.setBatchNum(task.batchNum);
		rowMap.setJobId(task.jobId);
		producer.push(rowMap);
		LOGGER.info("bootstrapping ended for " + task.logString());
	}

	private ResultSet getAllRows(String customSql,Connection connection) throws SQLException {
		Statement statement = createBatchStatement(connection);
		return statement.executeQuery(customSql);
	}

	private Statement createBatchStatement(Connection connection) throws SQLException {
		Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE);
		return statement;
	}

	private final String startBootstrapSQL = "update `"+ TablePrefixConfig.getTablePrefix() +"well_bootstrap` set started_at=NOW() where id=?";
	private void setBootstrapRowToStarted(Long id) throws SQLException, NoSuchElementException, DuplicateProcessException {
		this.context.getMaxwellConnectionPool().withSQLRetry(1, (connection) -> {
			PreparedStatement preparedStatement = connection.prepareStatement(startBootstrapSQL);
			preparedStatement.setLong(1, id);
			if ( preparedStatement.executeUpdate() == 0) {
				throw new NoSuchElementException();
			}
		});
	}

	private final String completeBootstrapSQL = "update `"+ TablePrefixConfig.getTablePrefix() +"well_bootstrap` set is_complete=1, inserted_rows=?, completed_at=NOW() where id=?";
	private void setBootstrapRowToCompleted(int insertedRows, Long id) throws SQLException, NoSuchElementException, DuplicateProcessException {
		this.context.getMaxwellConnectionPool().withSQLRetry(1, (connection) -> {
			PreparedStatement preparedStatement = connection.prepareStatement(completeBootstrapSQL);
			preparedStatement.setInt(1, insertedRows);
			preparedStatement.setLong(2, id);
			if (preparedStatement.executeUpdate() == 0) {
				throw new NoSuchElementException();
			}
		});
	}

	private Object getTimestamp(ResultSet resultSet, int columnIndex) throws SQLException {
		try {
			return resultSet.getTimestamp(columnIndex);
		} catch (SQLException e) {
			LOGGER.error("error trying to deserialize column at index: " + columnIndex);
			LOGGER.error("raw value:" + resultSet.getObject(columnIndex));
			throw(e);
		}
	}

	/**
	 * 自定义SQL时读取resultSet到RowMap
	 * @param row
	 * @param resultSet
	 */
	private void setRowValues4CustomSql(RowMap row, ResultSet resultSet) throws SQLException, ColumnDefCastException {
		ResultSetMetaData rsMeta=resultSet.getMetaData();
		int columnCount=rsMeta.getColumnCount();
		for (int i=1; i<=columnCount; i++) {
			int columnType=rsMeta.getColumnType(i);
			Object columnValue;
			// need to explicitly coerce TIME into TIMESTAMP in order to preserve nanoseconds
			if (columnType==92)
				columnValue = new DateUtil(resultSet.getTimestamp(i)).toShortDate();
			else if (columnType==93)
				columnValue = new DateUtil(resultSet.getTimestamp(i)).toLongDate();
			else if (columnType==91)
				columnValue = resultSet.getString(i);
			else
				columnValue = resultSet.getObject(i);

			row.putData(rsMeta.getColumnLabel(i), columnValue == null ? null : columnValue);
		}
	}

	private void setRowValues(RowMap row, ResultSet resultSet, Table table) throws SQLException, ColumnDefCastException {
		Iterator<ColumnDef> columnDefinitions = table.getColumnList().iterator();
		int columnIndex = 1;
		while ( columnDefinitions.hasNext() ) {
			ColumnDef columnDefinition = columnDefinitions.next();
			Object columnValue;

			// need to explicitly coerce TIME into TIMESTAMP in order to preserve nanoseconds
			if (columnDefinition instanceof TimeColumnDef)
				columnValue = getTimestamp(resultSet, columnIndex);
			else if ( columnDefinition instanceof DateColumnDef)
				columnValue = resultSet.getString(columnIndex);
			else
				columnValue = resultSet.getObject(columnIndex);

			row.putData(
				columnDefinition.getName(),
				columnValue == null ? null : columnDefinition.asJSON(columnValue, new MaxwellOutputConfig())
			);

			++columnIndex;
		}
	}

	private BootstrapTask getTaskById(Long id) throws SQLException {
		try ( Connection cx = this.context.getMaxwellConnectionPool().getConnection()) {
			PreparedStatement s = cx.prepareStatement("select * from "+ TablePrefixConfig.getTablePrefix() +"well_bootstrap where id = ?");
			s.setLong(1, id);
			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				return  BootstrapTask.valueOf(rs);
			}
		}
		return null;
	}
}
