package com.jingsky.mv.maxwell.replication;

import java.util.HashMap;

import com.jingsky.mv.maxwell.schema.Database;
import com.jingsky.mv.maxwell.filtering.Filter;
import com.jingsky.mv.maxwell.schema.Schema;
import com.jingsky.mv.maxwell.schema.Table;

public class TableCache {
	private final String maxwellDB;

	public TableCache(String maxwellDB) {
		this.maxwellDB = maxwellDB;
	}
	private final HashMap<Long, Table> tableMapCache = new HashMap<>();

	public void processEvent(Schema schema, Filter filter, Long tableId, String dbName, String tblName) {
		if ( !tableMapCache.containsKey(tableId)) {
			if ( filter.isTableBlacklisted(dbName, tblName) ) {
				return;
			}

			Database db = schema.findDatabase(dbName);
			if ( db == null )
				throw new RuntimeException("Couldn't find database " + dbName);
			else {
				Table tbl = db.findTable(tblName);

				if (tbl == null)
					throw new RuntimeException("Couldn't find table " + tblName + " in database " + dbName);
				else
					tableMapCache.put(tableId, tbl);
			}
		}

	}

	public Table getTable(Long tableId) {
		return tableMapCache.get(tableId);
	}

	public void clear() {
		tableMapCache.clear();
	}
}
