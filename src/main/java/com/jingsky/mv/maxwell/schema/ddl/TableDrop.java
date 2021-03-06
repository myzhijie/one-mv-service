package com.jingsky.mv.maxwell.schema.ddl;

import com.jingsky.mv.maxwell.schema.Database;
import com.jingsky.mv.maxwell.filtering.Filter;
import com.jingsky.mv.maxwell.schema.Schema;

public class TableDrop extends SchemaChange {
	public String database;
	final String table;
	final boolean ifExists;

	public TableDrop(String database, String table, boolean ifExists) {
		this.database = database;
		this.table = table;
		this.ifExists = ifExists;
	}

	@Override
	public ResolvedTableDrop resolve(Schema schema) {
		if ( ifExists ) {
			Database d = schema.findDatabase(this.database);
			if ( d == null || !d.hasTable(table) )
				return null;
		}

		return new ResolvedTableDrop(database, table);
	}

	@Override
	public boolean isBlacklisted(Filter filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isTableBlacklisted(this.database, this.table);
		}
	}

}
