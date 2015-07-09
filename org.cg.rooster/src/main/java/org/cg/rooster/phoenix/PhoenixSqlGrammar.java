package org.cg.rooster.phoenix;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cg.rooster.core.Condition;
import org.cg.rooster.core.SqlGrammar;
import org.cg.rooster.core.TableDefinition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;

import com.google.common.base.Preconditions;

/**
 * A {@link SqlGrammar} implementation to support Apache Phoenix specific SQL 
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
	public final static String VALUES = " VALUES ";
	public final static String UPSERT = "UPSERT INTO ";
	public final static String COMMA = ", ";
	public final static String SPACE = " ";
	public final static String PLACEHOLDER = "?";
	public final static String PLACEHOLDER_COMMA = "?, ";
	public final static String EQUAL_PLACEHOLDER = " = ?";
	public final static String ORDER_BY = " ORDER BY ";
	public final static String LIMIT = " LIMIT ";
	
	public final static String INTEGER = "INTEGER";
	public final static String BIGINT = "BIGINT";
	public final static String TINYINT = "TINYINT";
	public final static String SMALLINT = "SMALLINT";
	public final static String FLOAT = "FLOAT";
	public final static String DOUBLE = "DOUBLE";
	public final static String DECIMAL = "DECIMAL";
	public final static String BOOLEAN = "BOOLEAN";
	public final static String TIME = "TIME";
	public final static String DATE = "DATE";
	public final static String TIMESTAMP = "TIMESTAMP";
	public final static String VARCHAR = "VARCHAR";
	public final static String CHAR = "CHAR";
	public final static String BINARY = "BINARY";
	public final static String VARBINARY = "VARBINARY";
	public final static String ARRAY = "ARRAY";
	
	private static PhoenixSqlGrammar singleton = new PhoenixSqlGrammar( );

	private PhoenixSqlGrammar() { 

	}

	/**
	 * Get the singleton instance of PhoenixSqlGrammar
	 * @return
	 */
	public static PhoenixSqlGrammar getInstance( ) {
		return singleton;
	}
	
	@Override
	public String getParamDataType (Object arg) {
		if (arg instanceof Integer) {
			return INTEGER;
		} else if (arg instanceof Long) {
			return BIGINT;
		} else if (arg instanceof Byte) {
			return TINYINT;
		} else if (arg instanceof Short) {
			return SMALLINT;
		} else if (arg instanceof Float) {
			return FLOAT;
		} else if (arg instanceof Double) {
			return DOUBLE;
		} else if (arg instanceof java.math.BigDecimal) {
			return DECIMAL;
		} else if (arg instanceof Boolean) {
			return BOOLEAN;
		} else if (arg instanceof java.sql.Time) {
			return TIME;
		} else if (arg instanceof java.sql.Date) {
			return DATE;
		} else if (arg instanceof java.sql.Timestamp) {
			return TIMESTAMP;
		} else if (arg instanceof java.math.BigDecimal) {
			return DECIMAL;
		} else if (arg instanceof String) {
			return VARCHAR;
		} else if (arg instanceof byte[]) {
			return VARBINARY;
		} else if (arg instanceof java.sql.Array) {
			return ARRAY;
		} else {
			throw new UnsupportedOperationException("Data type unsupported."); 
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String count(TableDefinition table) {
		Preconditions.checkNotNull(table, "table must be provided");
		
		return SELECT + COUNT + FROM + table.getTableName();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String delete (TableDefinition table, int idSize) {
		Preconditions.checkNotNull(table, "table must be provided");
		
		String query = DELETE + FROM + table.getTableName();
		return query + whereByIdsClause(table, idSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String selectById (TableDefinition table, Sort sort, long limit, int idSize, final Map<String, String> dynamicColumnsType) {
		Preconditions.checkNotNull(table, "table must be provided");
		
		String query = SELECT + getColumnSelection(table) + FROM + table.getTableName();
		if (dynamicColumnsType!=null && !dynamicColumnsType.isEmpty()) {
			query = query + dynamicColumnsList(dynamicColumnsType);
		}
		if (idSize > 0) {
			query = query + whereByIdsClause(table, idSize);
		}
		return query + orderByClause(sort) + limitClause(limit);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String selectByCondition (TableDefinition table, Sort sort, long limit, final List<Condition> conditions,  final Map<String, String> dynamicColumnsType) {
		Preconditions.checkNotNull(table, "table must be provided");
		Preconditions.checkNotNull(conditions, "conditions must be provided");
		
		String query = SELECT + getColumnSelection(table) + FROM + table.getTableName();
		if (dynamicColumnsType!=null && !dynamicColumnsType.isEmpty()) {
			query = query + dynamicColumnsList(dynamicColumnsType);
		}
		if (conditions.size() > 0) {
			query = query + whereByConditionClause(table, conditions);
		}
		return query + orderByClause(sort) + limitClause(limit);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String save (TableDefinition table, final Map<String, Object> columnMapper, final Map<String, Object> dynamicColumnMapper) {
		Preconditions.checkNotNull(table, "table must be provided");
		Preconditions.checkNotNull(columnMapper, "columnMapper must be provided");
		Preconditions.checkNotNull(dynamicColumnMapper, "dynamicColumnMapper must be provided");
		
		final StringBuilder sb = new StringBuilder(UPSERT + table.getTableName() + " (");
		
		Iterator<Entry<String, Object>> iter = columnMapper.entrySet().iterator();
		Entry<String, Object> e;
		while ( iter.hasNext() ) {
			e = iter.next();
			sb.append(e.getKey());
			if (iter.hasNext()) {
				sb.append(COMMA);
			}
		}
		
		iter = dynamicColumnMapper.entrySet().iterator();
		if (iter.hasNext()) sb.append(COMMA);
		while ( iter.hasNext() ) {
			e = iter.next();
			sb.append(e.getKey()).append(SPACE).append( getParamDataType(e.getValue()) );
			if (iter.hasNext()) {
				sb.append(COMMA);
			}
		}
		sb.append(")").append(VALUES).append("(");
		
		//Construct parameter place holders for PreparedStatement
		int paramSize = columnMapper.size() + dynamicColumnMapper.size();
		for ( int i = 0; i < paramSize; i++ ) {
			sb.append(PLACEHOLDER);
			if ( i < (paramSize-1) ) {
				sb.append(COMMA);
			}
		}
		return sb.append(")").toString();
	}
	
	private static String getColumnSelection (TableDefinition table) {
		String columnsSelection = ALL;
		List<String> columnSelectionList = table.getColumnSelection();
		if ( columnSelectionList!=null && !columnSelectionList.isEmpty() ) {
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
	
	private static String limitClause(long limit) {
		if (limit<0) return "";
		return LIMIT + limit;
	}
	
	private static String orderByClause(Sort sort) {
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
	
	private static String whereByIdsClause(TableDefinition table, int idSize) {
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
	
	private static String whereByConditionClause(TableDefinition table, final List<Condition> conditions) {
		final StringBuilder sb = new StringBuilder(WHERE);

		for (int i = 0; i < conditions.size(); i++) {
			if (i > 0) sb.append(AND);
			sb.append("(");
			sb.append( Condition.parseCondition(conditions.get(i)) );
			sb.append(")");
		}
		return sb.toString();
	}
	
	private static String dynamicColumnsList(final Map<String, String> dynamicColumnsType) {
		final StringBuilder sb  = new StringBuilder("(");
		
		Iterator<Entry<String, String>> iter = dynamicColumnsType.entrySet().iterator();
		Entry<String, String> e;
		while ( iter.hasNext() ) {
			e = iter.next();
			sb.append(e.getKey()).append(SPACE).append(e.getValue());			
			if (iter.hasNext()) {
				sb.append(COMMA);
			}
		}
		sb.append(")");
		return sb.toString();
	}

}
