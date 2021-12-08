package com.jingsky.mv.maxwell.schema.ddl;

import com.jingsky.mv.maxwell.schema.Table;
import com.jingsky.mv.maxwell.schema.columndef.ColumnDef;

import java.util.List;

class ChangeColumnMod extends ColumnMod {
	public ColumnDef definition;
	public ColumnPosition position;

	public ChangeColumnMod(String name, ColumnDef d, ColumnPosition position ) {
		super(name);
		this.definition = d;
		this.position = position;
	}

	@Override
	public void apply(Table table, List<DeferredPositionUpdate> deferred) throws InvalidSchemaError {
		int idx = originalIndex(table);
		table.changeColumn(idx, position, definition, deferred);
	}
}

