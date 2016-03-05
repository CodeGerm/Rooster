package org.cg.rooster.core;

import java.util.List;

import org.springframework.data.domain.Sort;

import com.google.common.base.Preconditions;

/**
 * A builder to build a {@code Query} instance
 * @author WZ
 *
 */
public class QueryBuilder {
	
	private List<String> columnSelection = null;
	private List<Condition> conditions = null;
	private Sort sort = null;
	private int limit = Query.DEFAULT_QUERY_LIMIT;
	
	/**
	 * create a QueryBuilder instance
	 */
	public static QueryBuilder newBuilder() {
		return new QueryBuilder();
	}
	
	/**
	 * 
	 * @param columnSelection the column to select
	 * @return the builder
	 */
//  TODO support columnSelection
//	public QueryBuilder columnSelection(List<String> columnSelection) {
//		this.columnSelection = columnSelection;
//		return this;
//	}
		
	/**
	 * 
	 * @param conditions the conditions list
	 * @return the builder
	 */
	public QueryBuilder conditions(List<Condition> conditions) {
		this.conditions = conditions;
		return this;
	}
	
	/**
	 * 
	 * @param sort the sorting order
	 * @return the builder
	 */
	public QueryBuilder sort(Sort sort) {
		this.sort = sort;
		return this;
	}
	
	/**
	 * 
	 * @param limit the limit
	 * @return the builder
	 */
	public QueryBuilder limit(int limit) {
		Preconditions.checkArgument( (limit>0 && limit<=Query.DEFAULT_QUERY_LIMIT), "limit out of supported range");
		this.limit = limit;
		return this;
	}
	
	/**
	 * 
	 * @return the query
	 */
	public Query build() {
		return new Query(columnSelection, conditions, sort, limit);
	}

}
