package com.jingsky.mv.maxwell.recovery;

import com.jingsky.mv.maxwell.filtering.Filter;

/**
 * filter out (via a blacklist) everything except for `maxwell`.`t_trans_well_positions`.
 * this makes a possibly out of sync schema harmless.
 */
public class RecoveryFilter extends Filter {
	private final String maxwellDatabaseName;

	public RecoveryFilter(String maxwellDatabaseName) {
		this.maxwellDatabaseName = maxwellDatabaseName;
	}

	@Override
	public boolean isTableBlacklisted(String databaseName, String tableName) {
		return !(databaseName.equals(maxwellDatabaseName) && tableName.equals("heartbeats"));
	}
}
