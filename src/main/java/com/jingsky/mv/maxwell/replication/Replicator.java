package com.jingsky.mv.maxwell.replication;

import com.jingsky.mv.maxwell.schema.SchemaStoreException;
import com.jingsky.mv.maxwell.util.StoppableTask;
import com.jingsky.mv.maxwell.row.RowMap;
import com.jingsky.mv.maxwell.schema.Schema;

/**
 * Created by ben on 10/23/16.
 */
public interface Replicator extends StoppableTask {
	void startReplicator() throws Exception;
	RowMap getRow() throws Exception;
	Long getLastHeartbeatRead();
	Schema getSchema() throws SchemaStoreException;
	Long getSchemaId() throws SchemaStoreException;

	void stopAtHeartbeat(long heartbeat);
	void runLoop() throws Exception;
}
