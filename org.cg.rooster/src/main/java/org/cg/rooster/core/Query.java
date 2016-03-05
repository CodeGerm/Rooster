package org.cg.rooster.core;

import java.util.List;

import org.springframework.data.domain.Sort;

/**
 * A model that encapsulates all necessary parameters to construct a query 
 * @author WZ
 *
 */
public class Query {
	
	private final List<String> columnSelection;
	private final List<Condition> conditions;
	private final Sort sort;
	private final int limit;
	public final static int DEFAULT_QUERY_LIMIT = 5000;
	
	public Query(List<String> columnSelection, List<Condition> conditions, Sort sort, Integer limit) {
		super();
		this.columnSelection = columnSelection;
		this.conditions = conditions;
		this.sort = sort;
		this.limit = limit;
	}
	
	/**
	 * @return the columnSelection
	 */
	public List<String> getColumnSelection() {
		return columnSelection;
	}
	
	/**
	 * @return the conditions
	 */
	public List<Condition> getConditions() {
		return conditions;
	}
	
	/**
	 * @return the sort
	 */
	public Sort getSort() {
		return sort;
	}
	
	/**
	 * @return the limit
	 */
	public Integer getLimit() {
		return limit;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Query [columnSelection=" + columnSelection + ", conditions="
				+ conditions + ", sort=" + sort + ", limit=" + limit + "]";
	}
	
}
