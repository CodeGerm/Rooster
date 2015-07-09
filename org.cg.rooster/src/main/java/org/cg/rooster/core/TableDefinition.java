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
	private final int tenantId;
	
	private final boolean isMutable;
	private List<String> columnSelection;
	
	/**
	 * 
	 * @param tableName The table name
	 * @param primaryIdComponents The id components
	 */
	public TableDefinition(String tableName, String... primaryIdComponents) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName must be provided");
		Preconditions.checkArgument(primaryIdComponents.length > 0, "primaryIdComponent must be provided");
		this.tableName = tableName;
		this.primaryId = Collections.unmodifiableList(Arrays.asList(primaryIdComponents));
		this.tenantId = -1;
		this.isMutable = false;
		this.columnSelection = null;
	}
	
	/**
	 * 
	 * @param tableName The table name
	 * @param isMutable If the table is immutable or not.
	 * Note that there is no safeguards are in-place to enforce that a table declared as immutable during creation 
	 * (IMMUTABLE_ROWS=true) doesn't actually mutate data. 
	 * If that was to occur, the index would no longer be in sync with the table.
	 * Therefore, it's necessary to enforce it here to make sure it's insync with the index.
	 *
	 * @param primaryIdComponents The id components
	 */
	public TableDefinition(String tableName, boolean isMutable, String... primaryIdComponents) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName must be provided");
		Preconditions.checkArgument(primaryIdComponents.length > 0, "primaryIdComponent must be provided");
		this.tableName = tableName;
		this.primaryId = Collections.unmodifiableList(Arrays.asList(primaryIdComponents));
		this.tenantId = -1;
		this.isMutable = isMutable;
		this.columnSelection = null;
	}
	
	/**
	 * 
	 * @param tenantId The tenant id. By passing this, all the queries will be only running against the specified tenant.
	 * @param tableName The table name
	 * @param isMutable If the table is immutable or not.
	 * Note that there is no safeguards are in-place to enforce that a table declared as immutable during creation 
	 * (IMMUTABLE_ROWS=true) doesn't actually mutate data. 
	 * If that was to occur, the index would no longer be in sync with the table.
	 * Therefore, it's necessary to enforce it here to make sure it's insync with the index.
	 *
	 * @param primaryIdComponents The id components
	 */
	public TableDefinition(String tableName, int tenantId, boolean isMutable, String... primaryIdComponents) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName must be provided");
		Preconditions.checkArgument(primaryIdComponents.length > 0, "primaryIdComponent must be provided");
		this.tableName = tableName;
		this.primaryId = Collections.unmodifiableList(Arrays.asList(primaryIdComponents));
		this.tenantId = tenantId;
		this.isMutable = isMutable;
		this.columnSelection = null;
	}

	/**
	 * Set the column selection followed by the SELECT statement. By default it will select all ("SELECT *")
	 * This MUST be in sync with the RowColumnMapper.mapRow()
	 * @param columnSelection The column names to select
	 */
	public void setColumnSelection(String... columnSelection) {
		this.columnSelection = Collections.unmodifiableList(Arrays.asList(columnSelection));
	}

	public List<String> getColumnSelection() {
		return columnSelection;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public List<String> getPrimaryId() {
		return primaryId;
	}
	
	public int getTenantId() {
		return tenantId;
	}

	public boolean isMutable() {
		return isMutable;
	}

	@Override
	public String toString() {
		return "TableDefinition [tableName=" + tableName + ", primaryId="
				+ primaryId + ", tenantId=" + tenantId + ", isMutable="
				+ isMutable + "]";
	}

}
