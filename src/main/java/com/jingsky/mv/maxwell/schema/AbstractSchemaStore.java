package com.jingsky.mv.maxwell.schema;

import java.sql.SQLException;
import java.sql.Connection;
import java.util.List;
import java.util.ArrayList;

import com.jingsky.mv.maxwell.util.ConnectionPool;
import com.jingsky.mv.maxwell.CaseSensitivity;
import com.jingsky.mv.maxwell.filtering.Filter;
import com.jingsky.mv.maxwell.schema.ddl.InvalidSchemaError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jingsky.mv.maxwell.MaxwellContext;
import com.jingsky.mv.maxwell.schema.ddl.SchemaChange;
import com.jingsky.mv.maxwell.schema.ddl.ResolvedSchemaChange;

public abstract class AbstractSchemaStore {
	static final Logger LOGGER = LoggerFactory.getLogger(AbstractSchemaStore.class);
	protected final ConnectionPool replicationConnectionPool;
	protected final ConnectionPool schemaConnectionPool;
	protected final CaseSensitivity caseSensitivity;
	private final Filter filter;

	protected AbstractSchemaStore(ConnectionPool replicationConnectionPool,
								  ConnectionPool schemaConnectionPool,
								  CaseSensitivity caseSensitivity,
								  Filter filter) {
		this.replicationConnectionPool = replicationConnectionPool;
		this.schemaConnectionPool = schemaConnectionPool;
		this.caseSensitivity = caseSensitivity;
		this.filter = filter;
	}

	protected AbstractSchemaStore(MaxwellContext context) throws SQLException {
		this(context.getReplicationConnectionPool(), context.getSchemaConnectionPool(), context.getCaseSensitivity(), context.getFilter());
	}

	protected Schema captureSchema() throws SQLException {
		try(Connection connection = schemaConnectionPool.getConnection()) {
			LOGGER.info("Maxwell is capturing initial schema");
			SchemaCapturer capturer = new SchemaCapturer(connection, caseSensitivity);
			return capturer.capture();
		}
	}

	protected List<ResolvedSchemaChange> resolveSQL(Schema schema, String sql, String currentDatabase) throws InvalidSchemaError {
		List<SchemaChange> changes = SchemaChange.parse(currentDatabase, sql);

		if ( changes == null || changes.size() == 0 )
			return new ArrayList<>();

		ArrayList<ResolvedSchemaChange> resolvedSchemaChanges = new ArrayList<>();

		for ( SchemaChange change : changes ) {
			if ( !change.isBlacklisted(this.filter) ) {
				ResolvedSchemaChange resolved = change.resolve(schema);
				if ( resolved != null ) {
					resolved.apply(schema);

					resolvedSchemaChanges.add(resolved);
				}
			} else {
				LOGGER.debug("ignoring blacklisted schema change");
			}
		}
		return resolvedSchemaChanges;
	}
}


