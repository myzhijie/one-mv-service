package com.jingsky.mv.maxwell.recovery;

import com.jingsky.mv.maxwell.replication.Position;
import com.jingsky.mv.maxwell.schema.Schema;
import com.jingsky.mv.maxwell.schema.SchemaCapturer;
import com.jingsky.mv.maxwell.schema.SchemaStore;
import com.jingsky.mv.maxwell.schema.SchemaStoreException;
import com.jingsky.mv.maxwell.util.ConnectionPool;
import com.jingsky.mv.maxwell.CaseSensitivity;
import com.jingsky.mv.maxwell.schema.ddl.InvalidSchemaError;
import com.jingsky.mv.maxwell.schema.ddl.ResolvedSchemaChange;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A schema store that contains only the maxwell database, and throws
 * away any schema changes we encounter while trying to recover the binlog position.
 */
public class RecoverySchemaStore implements SchemaStore {

	private ConnectionPool replicationConnectionPool;
	private String maxwellDatabaseName;
	private CaseSensitivity caseSensitivity;
	private Schema maxwellOnlySchema;

	public RecoverySchemaStore(ConnectionPool replicationConnectionPool,
							   String maxwellDatabaseName,
							   CaseSensitivity caseSensitivity) {

		this.replicationConnectionPool = replicationConnectionPool;
		this.maxwellDatabaseName = maxwellDatabaseName;
		this.caseSensitivity = caseSensitivity;
	}

	@Override
	public Schema getSchema() throws SchemaStoreException {
		if ( maxwellOnlySchema != null )
			return maxwellOnlySchema;

		try(Connection conn = replicationConnectionPool.getConnection() ) {
			SchemaCapturer capturer = new SchemaCapturer(conn, caseSensitivity, maxwellDatabaseName);
			maxwellOnlySchema = capturer.capture();
		} catch (SQLException e) {
			throw new SchemaStoreException(e);
		}

		return maxwellOnlySchema;
	}

	@Override
	public List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, Position position) throws SchemaStoreException, InvalidSchemaError {
		return new ArrayList<>();
	}

	@Override
	public Long getSchemaID() throws SchemaStoreException {
		return new Long(0);
	}
}
