package com.jingsky.mv.maxwell.row;

import com.jingsky.mv.maxwell.producer.MaxwellOutputConfig;
import com.jingsky.mv.maxwell.replication.Position;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by ben on 9/7/16.
 */
public class HeartbeatRowMap extends RowMap {
	public HeartbeatRowMap(String database, Position position, Position nextPosition) {
		super("heartbeat", database, "heartbeats", position.getLastHeartbeatRead(), new ArrayList<String>(), position, nextPosition, null);
	}

	public static HeartbeatRowMap valueOf(String database, Position position, Position nextPosition) {
		return new HeartbeatRowMap(database, position, nextPosition);
	}

	@Override
	public String toJSON(MaxwellOutputConfig outputConfig) throws IOException {
		return null;
	}

	@Override
	public String toJSON() throws IOException {
		return null;
	}

	@Override
	public boolean isTXCommit() {
		return true;
	}

	@Override
	public boolean shouldOutput(MaxwellOutputConfig outputConfig) {
		return false;
	}
}
