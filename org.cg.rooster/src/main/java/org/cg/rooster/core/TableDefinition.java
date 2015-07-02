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
	
	public TableDefinition(String tableName, String... primaryIdComponents) {
		super();
		Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName must be provided");
		Preconditions.checkArgument(primaryIdComponents.length > 0, "primaryIdComponent must be provided");
		this.tableName = tableName;
		this.primaryId = Collections.unmodifiableList(Arrays.asList(primaryIdComponents));
		this.tenantId = -1;
		this.isMutable = false;
		this.columnSelection = null;
	}
	
	public TableDefinition(String tableName, boolean isMutable, String... primaryIdComponents) {
		super();
		Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName must be provided");
		Preconditions.checkArgument(primaryIdComponents.length > 0, "primaryIdComponent must be provided");
		this.tableName = tableName;
		this.primaryId = Collections.unmodifiableList(Arrays.asList(primaryIdComponents));
		this.tenantId = -1;
		this.isMutable = isMutable;
		this.columnSelection = null;
	}
	
	public TableDefinition(String tableName, int tenantId, boolean isMutable, String... primaryIdComponents) {
		super();
		Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName must be provided");
		Preconditions.checkArgument(primaryIdComponents.length > 0, "primaryIdComponent must be provided");
		this.tableName = tableName;
		this.primaryId = Collections.unmodifiableList(Arrays.asList(primaryIdComponents));
		this.tenantId = tenantId;
		this.isMutable = isMutable;
		this.columnSelection = null;
	}
	
	public List<String> getColumnSelection() {
		return columnSelection;
	}

	public void setColumnSelection(List<String> columnSelection) {
		this.columnSelection = columnSelection;
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
