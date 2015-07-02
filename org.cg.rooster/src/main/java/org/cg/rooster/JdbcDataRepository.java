package org.cg.rooster;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.cg.rooster.core.DynamicRowColumnMapper;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.SqlGrammar;
import org.cg.rooster.core.TableDefinition;
import org.springframework.data.domain.Persistable;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A generic JDBC data repository that implements {@link PagingAndSortingRepository}
 * @author WZ
 *
 * @param <T> entity type
 * @param <ID> entity id type
 */
public abstract class JdbcDataRepository <T extends Persistable<ID>, ID extends Serializable> implements DataRepository<T, ID> {

	private final TableDefinition tableDefinition;
	private final RowColumnMapper<T> rowColumnMapper;
	private final DynamicRowColumnMapper<T> dynamicRowColumnMapper;
	private final JdbcTemplate jdbcTemplate;

	private SqlGrammar sqlGrammar;
	private DataSource dataSource;
	
	//TODO will be configurable
	private final static long DEFAULT_QUERY_LIMIT = 1000;
	
	public static Object[] primaryKey(Object... idValues) {
		return idValues;
	}
	
	public JdbcDataRepository(TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper) {
		this(tableDefinition, rowColumnMapper, null);
	}
	
	public JdbcDataRepository(TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper, DynamicRowColumnMapper<T> dynamicRowColumnMapper) {
		super();
		Preconditions.checkNotNull(tableDefinition, "tableDefinition must be provided");
		Preconditions.checkNotNull(rowColumnMapper, "rowColumnMapper must be provided");
		this.tableDefinition = tableDefinition;
		this.rowColumnMapper = rowColumnMapper;
		this.dynamicRowColumnMapper = dynamicRowColumnMapper;
		init();
		
		Preconditions.checkState(sqlGrammar!=null, "sqlGrammar must be initialized with inti()");
		Preconditions.checkState(dataSource!=null, "dataSource must be initialized with inti()");
		
		jdbcTemplate = new JdbcTemplate(dataSource);
	}
	
	/**
	 * Implement this method to initialize SqlGrammar and DataSource
	 */
	public abstract void init ();
	
	protected TableDefinition getTableDefinition() {
		return tableDefinition;
	}
	
	protected JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	protected RowColumnMapper<T> getRowColumnMapper() {
		return rowColumnMapper;
	}

	protected DynamicRowColumnMapper<T> getDynamicRowColumnMapper() {
		return dynamicRowColumnMapper;
	}

	protected SqlGrammar getSqlGrammar() {
		return sqlGrammar;
	}

	protected void setSqlGrammar(SqlGrammar sqlGrammar) {
		Preconditions.checkNotNull(sqlGrammar, "sqlGrammar must be provided");
		this.sqlGrammar = sqlGrammar;
	}
	
	protected DataSource getDataSource() {
		return dataSource;
	}

	protected void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public <S extends T> S save(S entity) {
		Preconditions.checkNotNull(entity, "entity must be provided");
		
		final Map<String, Object> columns = new LinkedHashMap<String, Object>(rowColumnMapper.mapColumns(entity));	
		final Object[] queryParams = columns.values().toArray();
		final String createQuery = sqlGrammar.save(tableDefinition, columns.keySet());
		getJdbcTemplate().update(createQuery, queryParams);
		return entity;
	}

	@Override
	public <S extends T> Iterable<S> save(Iterable<S> entities) {
		Preconditions.checkNotNull(entities, "entities must be provided");
		
		final List<S> entityList = Lists.newArrayList(entities);
		
		//TODO may look ugly but we only need to get the key set from the first entity
		final Map<String, Object> firstColumns = new LinkedHashMap<String, Object>(rowColumnMapper.mapColumns(entityList.get(0)));
		final String createQuery = sqlGrammar.save(tableDefinition, firstColumns.keySet());
		
		List<Object[]> batchArgs = new LinkedList<Object[]>();
		for (S entity : entityList) {
			final Map<String, Object> columns = new LinkedHashMap<String, Object>(rowColumnMapper.mapColumns(entity));	
			final Object[] queryParams = columns.values().toArray();
			batchArgs.add(queryParams);
		}
		
		getJdbcTemplate().batchUpdate(createQuery, batchArgs);
		
		List<S> result = new LinkedList<S>();
		Iterator<S> iter = entities.iterator();
	    while ( iter.hasNext() ){
	    	result.add( save(iter.next()) );
	    }
		return result;
	}

	@Override
	public boolean exists(ID id) {
		Preconditions.checkNotNull(id, "id must be provided");
		
		return this.findOne(id) != null;
	}
	
	@Override
	public long count() {
		return getJdbcTemplate().queryForObject(sqlGrammar.count(tableDefinition), Long.class);
	}
	
	@Override
	public void delete(ID id) {
		Preconditions.checkNotNull(id, "id must be provided");
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");
		
		final Object[] idColumns = (id instanceof Object[]) ? (Object[]) id : new Object[]{id};
		getJdbcTemplate().update(sqlGrammar.delete(tableDefinition), idColumns);
	}

	@Override
	public void delete(T entity) {
		Preconditions.checkNotNull(entity, "entity must be provided");
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");
		
		this.delete(entity.getId());
	}

	@Override
	public void delete(Iterable<? extends T> entities) {
		Preconditions.checkNotNull(entities, "entities must be provided");
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");

		Iterator<? extends T> iter = entities.iterator();
	    while ( iter.hasNext() ){
	      this.delete( iter.next() );
	    }
	}

	@Override
	public void deleteAll() {
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");

		getJdbcTemplate().update(sqlGrammar.deleteAll(tableDefinition));
	}
	
	@Override
	public T findOne(ID id) {
		Preconditions.checkNotNull(id, "id must be provided");
		
		final Object[] idColumns = (id instanceof Object[]) ? (Object[]) id : new Object[]{id};
		final List<T> entity = jdbcTemplate.query(sqlGrammar.select(tableDefinition, null, DEFAULT_QUERY_LIMIT, 1), idColumns, rowColumnMapper);
		return entity.isEmpty() ? null : entity.get(0);
	}

	@Override
	public Iterable<T> findAll() {
		return getJdbcTemplate().query(sqlGrammar.select(tableDefinition, null, DEFAULT_QUERY_LIMIT, -1), rowColumnMapper);
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {
		Preconditions.checkNotNull(ids, "ids must be provided");
		
		final List<ID> idsList = Lists.newLinkedList(ids);
		if (idsList.isEmpty()) {
			return Collections.emptyList();
		}
		
		//need to put all id components for all ids in a single flat array
		final List<Object> idColumnValuesList = new LinkedList<Object>();
		for (ID id : ids) {
			List<Object> idList;
			if (id instanceof Object[]) {
				idList = Arrays.asList((Object[]) id);
			} else {
				idList = Collections.<Object>singletonList(id);
			}
			idColumnValuesList.addAll(idList);
		}		
		
		return getJdbcTemplate().query(sqlGrammar.select(tableDefinition, null, DEFAULT_QUERY_LIMIT, idsList.size()), rowColumnMapper, idColumnValuesList.toArray());
	}
	
	@Override
	public Iterable<T> findAll (Sort sort) {
		Preconditions.checkNotNull(sort, "sort must be provided");
		
		return findAll(sort, DEFAULT_QUERY_LIMIT);
	}
	
	@Override
	public Iterable<T> findAll (long limit) {
		Preconditions.checkArgument( (limit>0 && limit<DEFAULT_QUERY_LIMIT), "limit out of range");
		
		return findAll(null, limit);
	}
	
	@Override
	public Iterable<T> findAll(Sort sort, long limit) {		
		return getJdbcTemplate().query(sqlGrammar.select(tableDefinition, sort, limit, -1), rowColumnMapper);
	}
	
	@Override
	public Iterable<T> find (final Map<String, Object> valueMapping) {
		Preconditions.checkNotNull(valueMapping, "valueMapping must be provided");
		
		return find(valueMapping, null, DEFAULT_QUERY_LIMIT);
	}
	
	@Override
	public Iterable<T> find (final Map<String, Object> valueMapping, long limit) {
		Preconditions.checkNotNull(valueMapping, "valueMapping must be provided");
		Preconditions.checkArgument( (limit>0 && limit<DEFAULT_QUERY_LIMIT), "limit out of range");
		
		return find(valueMapping, null, limit);
	}
	
	@Override
	public Iterable<T> find (final Map<String, Object> valueMapping, Sort sort) {
		Preconditions.checkNotNull(valueMapping, "valueMapping must be provided");
		Preconditions.checkNotNull(sort, "sort must be provided");
		
		return find(valueMapping, sort, DEFAULT_QUERY_LIMIT);
	}
	
	@Override
	public Iterable<T> find (final Map<String, Object> valueMapping, Sort sort, long limit) {
		Preconditions.checkNotNull(valueMapping, "valueMapping must be provided");
		Preconditions.checkArgument( (limit>0 && limit<DEFAULT_QUERY_LIMIT), "limit out of range");
		
		return null;
	}

}
