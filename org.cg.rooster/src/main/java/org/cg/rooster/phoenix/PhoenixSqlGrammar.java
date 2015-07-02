package org.cg.rooster.phoenix;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.cg.rooster.core.SqlGrammar;
import org.cg.rooster.core.TableDefinition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;

import com.google.common.base.Preconditions;

/**
 * A Phoenix supported SQL Grammar implementation of SqlGrammar 
 * @author WZ
 *
 */
public class PhoenixSqlGrammar implements SqlGrammar {

	public final static String SELECT = "SELECT ";
	public final static String COUNT = "COUNT(*) ";
	public final static String ALL = "* ";
	public final static String FROM = "FROM ";
	public final static String WHERE = " WHERE ";
	public final static String AND = " AND ";
	public final static String OR = " OR ";
	public final static String DELETE = "DELETE ";
	public final static String VALUES = "VALUES ";
	public final static String UPSERT = "UPSERT INTO ";
	public final static String COMMA = ", ";
	public final static String SPACE = " ";
	public final static String PLACEHOLDER = "?";
	public final static String PLACEHOLDER_COMMA = "?, ";
	public final static String EQUAL_PLACEHOLDER = " = ?";
	public final static String ORDER_BY = " ORDER BY ";
	public final static String LIMIT = " LIMIT ";
	
	@Override
	public String count(TableDefinition table) {
		Preconditions.checkNotNull(table, "table must be provided");
		
		return SELECT + COUNT + FROM + table.getTableName();
	}

	@Override
	public String delete(TableDefinition table) {
		Preconditions.checkNotNull(table, "table must be provided");
		
		return DELETE + FROM + table.getTableName() + whereByIdClause(table);
	}

	@Override
	public String deleteAll(TableDefinition table) {
		Preconditions.checkNotNull(table, "table must be provided");
		
		return DELETE + FROM + table.getTableName();
	}

	@Override
	public String select (TableDefinition table, Sort sort, long limit, int idSize) {
		Preconditions.checkNotNull(table, "table must be provided");
		
		String query = SELECT + getColumnSelection(table) + FROM + table.getTableName();
		if (idSize > 0) {
			return query + whereByIdsClause(table, idSize);
		}
		return query + orderByClause(sort) + limitClause(limit);
	}

	@Override
	public String save(TableDefinition table, Set<String> columnSet) {
		Preconditions.checkNotNull(table, "table must be provided");
		Preconditions.checkNotNull(columnSet, "columnSet must be provided");
		
		final StringBuilder sb = new StringBuilder(UPSERT + table.getTableName() + " (");
		
		Iterator<String> iter = columnSet.iterator();
		while ( iter.hasNext() ) {
			final String column = iter.next();
			sb.append(column);
			if (iter.hasNext()) {
				sb.append(COMMA);
			}
		}
		sb.append(")").append(VALUES).append("(");
		
		Iterator<String> iterAgain = columnSet.iterator();
		while ( iterAgain.hasNext() ) {
			iterAgain.next();
			sb.append(PLACEHOLDER);
			if (iterAgain.hasNext()) {
				sb.append(COMMA);
			}
		}
		return sb.append(")").toString();
	}
	
	//TODO get from row mapper?
	private String getColumnSelection (TableDefinition table) {
		String columnsSelection = ALL;
		List<String> columnSelectionList = table.getColumnSelection();
		if ( columnSelectionList!=null && columnSelectionList.isEmpty() ) {
			StringBuilder sb = new StringBuilder();
			Iterator<String> iter = columnSelectionList.iterator();
			while ( iter.hasNext() ) {
				final String column = iter.next();
				sb.append(column);
				if (iter.hasNext()) {
					sb.append(COMMA);
				} else {
					sb.append(SPACE);
				}
			}
			columnsSelection = sb.toString();
		}
		return columnsSelection;
	}
	
	private String limitClause(long limit) {
		if (limit<0) return "";
		return LIMIT + limit;
	}
	
	private String orderByClause(Sort sort) {
		if (sort == null) return "";
		StringBuilder sb = new StringBuilder();
		sb.append(ORDER_BY);
		
		Iterator<Order> iter = sort.iterator();
		while ( iter.hasNext() ) {
			final Order order = iter.next();
			sb.append(order.getProperty()).append(SPACE).append(order.getDirection().toString());
			if (iter.hasNext()) {
				sb.append(COMMA);
			}
		}
		return sb.toString();
	}
	
	//TODO add non-id columns support
	private String whereByIdClause(TableDefinition table) {
		final StringBuilder sb = new StringBuilder(WHERE);
		
		Iterator<String> iter = table.getPrimaryId().iterator();
		while ( iter.hasNext() ) {
			sb.append(iter.next()).append(EQUAL_PLACEHOLDER);
			if (iter.hasNext()) {
				sb.append(AND);
			} else {
				sb.append(SPACE);
			}
		}
		return sb.toString();
	}
	
	private String whereByIdsClause(TableDefinition table, int idSize) {
		final List<String> idComponents = table.getPrimaryId();		
		final StringBuilder sb = new StringBuilder(WHERE);

		for (int i = 0; i < idSize; i++) {
			if (i > 0) sb.append(OR);
			sb.append("(");
			Iterator<String> iter = idComponents.iterator();
			while ( iter.hasNext() ) {
				sb.append(iter.next()).append(EQUAL_PLACEHOLDER);
				if (iter.hasNext()) {
					sb.append(AND);
				} else {
					sb.append(SPACE);
				}
			}
			sb.append(")");
		}
		return sb.toString();
	}
}
