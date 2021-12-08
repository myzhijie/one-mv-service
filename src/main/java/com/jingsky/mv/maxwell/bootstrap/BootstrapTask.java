package com.jingsky.mv.maxwell.bootstrap;

import com.jingsky.mv.maxwell.replication.BinlogPosition;
import com.jingsky.mv.maxwell.row.RowMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class BootstrapTask {
	public String database;
	public String table;
	public Integer repeatOrder;
	public Integer batchNum;
	public String customSql;
	public Long id;
	public BinlogPosition startPosition;
	public boolean complete;
	public Timestamp startedAt;
	public Timestamp completedAt;
	public String comment;
	//批次ID，每次bootstrap对应一个总任务ID
	public String jobId;
	public volatile boolean abort;


	public String logString() {
		String s = String.format("#%d %s.%s", id, database, table);
		if ( customSql != null )
			s += " customSql:" + customSql;
		return s;
	}

	static BootstrapTask valueOf(ResultSet rs) throws SQLException {
		BootstrapTask task = new BootstrapTask();
		task.id = rs.getLong("id");
		task.database = rs.getString("database_name");
		task.table = rs.getString("table_name");
		task.customSql = rs.getString("custom_sql");
		task.repeatOrder = rs.getInt("repeat_order");
		task.batchNum = rs.getInt("batch_num");
		task.startPosition = null;
		task.complete = rs.getBoolean("is_complete");
		task.completedAt = rs.getTimestamp("completed_at");
		task.startedAt = rs.getTimestamp("started_at");
		task.comment = rs.getString("comment");
		return task;
	}

	public static BootstrapTask valueOf(RowMap row) {
		BootstrapTask t = new BootstrapTask();
		t.database = (String) row.getData("database_name");
		t.table = (String) row.getData("table_name");
		t.customSql = (String) row.getData("custom_sql");
		t.repeatOrder = (Integer) row.getData("repeat_order");
		t.batchNum = (Integer) row.getData("batch_num");
		t.id = (Long) row.getData("id");

		String binlogFile = (String) row.getData("binlog_file");
		Long binlogOffset = (Long) row.getData("binlog_position");

		t.startPosition = BinlogPosition.at(binlogOffset, binlogFile);
		return t;
	}

	public boolean matches(RowMap row) {
		return database.equalsIgnoreCase(row.getDatabase())
			&& table.equalsIgnoreCase(row.getTable());
	}
}
