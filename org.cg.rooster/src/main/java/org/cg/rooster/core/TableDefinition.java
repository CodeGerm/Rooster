package org.cg.rooster.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Define a table with its properties
 * @author WZ
 *
 */
public class TableDefinition {
	
	private final String tableName;
	private final List<String> primaryId;
	private final boolean isMutable;
	private final boolean isReadonly;
	
	/**
	 * @param tableName The table name
	 * @param primaryIdComponents The id components
	 */
	public TableDefinition(String tableName, String... primaryIdComponents) {
		this(false, tableName, false, primaryIdComponents);
	}
	
	/**
	 * @param isReadonly If the table is readonly or not
	 * @param tableName The table name
	 * @param primaryIdComponents The id components
	 */
	public TableDefinition(boolean isReadonly, String tableName, String... primaryIdComponents) {
		this(isReadonly, tableName, false, primaryIdComponents);
	}
	
	/**
	 * @param tableName The table name
	 * @param isMutable If the table is mutable or not.
	 * Note that there is no safeguards are in-place to enforce that a table declared as immutable during creation 
	 * (IMMUTABLE_ROWS=true) doesn't actually mutate data. 
	 * If that was to occur, the index would no longer be in sync with the table.
	 * Therefore, it's necessary to enforce it here to make sure it's insync with the index.
	 *
	 * @param primaryIdComponents The id components
	 */
	public TableDefinition(String tableName, boolean isMutable, String... primaryIdComponents) {
		this(false, tableName, isMutable, primaryIdComponents);
	}
	
	/**
	 * @param isReadonly If the table is readonly or not
	 * @param tableName The table name
	 * @param isMutable If the table is mutable or not.
	 * Note that there is no safeguards are in-place to enforce that a table declared as immutable during creation 
	 * (IMMUTABLE_ROWS=true) doesn't actually mutate data. 
	 * If that was to occur, the index would no longer be in sync with the table.
	 * Therefore, it's necessary to enforce it here to make sure it's insync with the index.
	 *
	 * @param primaryIdComponents The id components
	 */
	public TableDefinition(boolean isReadonly, String tableName, boolean isMutable, String... primaryIdComponents) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName must be provided");
		Preconditions.checkArgument(primaryIdComponents.length > 0, "primaryIdComponent must be provided");
		this.tableName = tableName;
		this.primaryId = Collections.unmodifiableList(Arrays.asList(primaryIdComponents));
		this.isMutable = isMutable;
		this.isReadonly = isReadonly;
	}

	public String getTableName() {
		return tableName;
	}
	
	public List<String> getPrimaryId() {
		return primaryId;
	}
	
	public boolean isMutable() {
		return isMutable;
	}
	
	public boolean isReadonly() {
		return isReadonly;
	}

	@Override
	public String toString() {
		return "TableDefinition [tableName=" + tableName + ", primaryId="
				+ primaryId + ", isMutable="
				+ isMutable + "]";
	}

}
