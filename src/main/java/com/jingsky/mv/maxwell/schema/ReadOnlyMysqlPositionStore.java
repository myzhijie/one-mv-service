package com.jingsky.mv.maxwell.schema;

import com.jingsky.mv.maxwell.util.ConnectionPool;
import com.jingsky.mv.maxwell.replication.Position;

/**
 * a schema position object that doesn't write its position out.
 * useful for "replay" mode.
 */
public class ReadOnlyMysqlPositionStore extends MysqlPositionStore {
	public ReadOnlyMysqlPositionStore(ConnectionPool pool, Long serverID, String clientID, boolean gtidMode) {
		super(pool, serverID, clientID, gtidMode);
	}

	@Override
	public void set(Position p) { }

	@Override
	public long heartbeat() throws Exception {
		return System.currentTimeMillis();
	}
}
