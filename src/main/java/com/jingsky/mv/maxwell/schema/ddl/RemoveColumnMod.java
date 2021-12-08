package com.jingsky.mv.maxwell.schema.ddl;

import com.jingsky.mv.maxwell.schema.Table;

import java.util.List;

class RemoveColumnMod extends ColumnMod {
	public RemoveColumnMod(String name) {
		super(name);
	}

	@Override
	public void apply(Table table, List<DeferredPositionUpdate> deferred) throws InvalidSchemaError {
		table.removeColumn(originalIndex(table));
	}
}
