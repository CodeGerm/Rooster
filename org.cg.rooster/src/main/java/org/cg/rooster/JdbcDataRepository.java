package org.cg.rooster;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.rooster.core.Condition;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.SqlGrammar;
import org.cg.rooster.core.TableDefinition;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Persistable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * A generic JDBC data repository that implements {@link PagingAndSortingRepository}
 * @author WZ
 *
 * @param <T> entity type
 * @param <ID> entity id type (for compound id, just use Object [])
 */
public abstract class JdbcDataRepository <T extends Persistable<ID>, ID extends Serializable> implements DataRepository<T, ID> {

	private static final Log LOG = LogFactory.getLog(JdbcDataRepository.class);

	private final TableDefinition tableDefinition;
	private final RowColumnMapper<T> rowColumnMapper;
	private final JdbcTemplate jdbcTemplate;
	private final SqlGrammar sqlGrammar;
	private final PlatformTransactionManager transactionManager;
	
	//TODO will be configurable
	private final static long DEFAULT_QUERY_LIMIT = 5000;
	
	/**
	 * Get as primary key
	 * 
	 * @param idValues
	 * @return a object array of primary key
	 */
	public static Object[] primaryKey (Object... idValues) {
		return idValues;
	}
	
	/**
	 * Constructor
	 * 
	 * @param tableDefinition the table definition
	 * @param rowColumnMapper the row column mapper 
	 */
	public JdbcDataRepository (TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper, DataSource dataSource, SqlGrammar sqlGrammar) {
		super();
		Preconditions.checkNotNull(tableDefinition, "tableDefinition must be provided");
		Preconditions.checkNotNull(rowColumnMapper, "rowColumnMapper must be provided");
		Preconditions.checkNotNull(dataSource, "dataSource must be provided");
		Preconditions.checkNotNull(sqlGrammar, "sqlGrammar must be provided");
		
		this.tableDefinition = tableDefinition;
		this.rowColumnMapper = rowColumnMapper;		
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.jdbcTemplate.setFetchSize(1000);
		this.sqlGrammar = sqlGrammar;
		this.transactionManager = new DataSourceTransactionManager(dataSource);
	}
	
	protected TableDefinition getTableDefinition() {
		return tableDefinition;
	}
	
	protected JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
	
	protected RowColumnMapper<T> getRowColumnMapper() {
		return rowColumnMapper;
	}

	protected SqlGrammar getSqlGrammar() {
		return sqlGrammar;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <S extends T> S save (S entity) {
		Preconditions.checkNotNull(entity, "entity must be provided");
		Preconditions.checkState(rowColumnMapper!=null, "rowColumnMapper must be initiated");

		final Map<String, Object> columnMap = rowColumnMapper.mapColumns(entity);		
		final Map<String, Object> dynamicColumnMap = rowColumnMapper.mapDynamicColumns(entity);
		
		Preconditions.checkState(columnMap!=null, "rowColumnMapper.mapColumns must be implemented");
		Preconditions.checkState(dynamicColumnMap!=null, "rowColumnMapper.mapDynamicColumns cannot cannot return null");
				
		this.transactionalUpdate(
				sqlGrammar.save(tableDefinition, columnMap, dynamicColumnMap), 
				ArrayUtils.addAll(columnMap.values().toArray(), dynamicColumnMap.values().toArray()));	
		
		LOG.info(String.format("entity saved: %s.", entity));
		return entity;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <S extends T> Iterable<S> save (final Iterable<S> entities) {
		Preconditions.checkNotNull(entities, "entities must be provided");
		Preconditions.checkState(rowColumnMapper!=null, "rowColumnMapper must be initiated");
		
		List<Object[]> batchArgs = new LinkedList<Object[]>();
		Map<String, Object> columns;
		Map<String, Object> dynamicColumns;
		String createQuery = "";
		Iterator<S> iter = entities.iterator();
		S entity;
		while (iter.hasNext()) {
			entity = iter.next();
			columns = rowColumnMapper.mapColumns(entity);
			dynamicColumns = rowColumnMapper.mapDynamicColumns(entity);
			batchArgs.add(ArrayUtils.addAll(columns.values().toArray(), dynamicColumns.values().toArray()));
			if (!iter.hasNext()) {
				// In order to get column mapping for query construction, 
				// we only need to get the mapping from the last entity.
				Map<String, Object> columnMap = rowColumnMapper.mapColumns(entity);
				Map<String, Object> dynamicColumnMap = rowColumnMapper.mapDynamicColumns(entity);
				Preconditions.checkState(columnMap!=null, "rowColumnMapper.mapColumns must be implemented");
				Preconditions.checkState(dynamicColumnMap!=null, "rowColumnMapper.mapColumns must be implemented");
				createQuery = sqlGrammar.save(tableDefinition, columnMap, dynamicColumnMap);
			}
		}
		long start = System.currentTimeMillis();
		this.transactionalBatchUpdate(createQuery, batchArgs);
		long end = System.currentTimeMillis() - start;
		LOG.info(String.format("saved %s entities in %sms", batchArgs.size(), end));
		return entities;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean exists (ID id) {
		Preconditions.checkNotNull(id, "id must be provided");
		
		return this.find(id) != null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long count() {
		return getJdbcTemplate().queryForObject(sqlGrammar.count(tableDefinition), Long.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete (ID id) {
		Preconditions.checkNotNull(id, "id must be provided");
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");
		
		final Object[] idColumns = (id instanceof Object[]) ? (Object[]) id : new Object[]{id};
		this.transactionalUpdate(sqlGrammar.delete(tableDefinition), idColumns);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete (final Iterable<ID> ids) {
		Preconditions.checkNotNull(ids, "ids must be provided");
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");
		
		Iterator<ID> iter = ids.iterator();
		Preconditions.checkArgument(iter.hasNext(), "ids must be provided");

		List<Object[]> batchArgs = new LinkedList<Object[]>();
		ID id;
		while (iter.hasNext()) {
			id = iter.next();
			if (id instanceof Object[]) {
				batchArgs.add((Object[]) id);
			} else {
				batchArgs.add(new Object[]{id});
			}
		}
		this.transactionalBatchUpdate(sqlGrammar.delete(tableDefinition), batchArgs);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> findAll () {		
		return findAll(null, DEFAULT_QUERY_LIMIT);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> findAll (Sort sort) {
		Preconditions.checkNotNull(sort, "sort must be provided");
		
		return findAll(sort, DEFAULT_QUERY_LIMIT);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> findAll (long limit) {
		Preconditions.checkArgument( (limit>0 && limit<=DEFAULT_QUERY_LIMIT), "limit out of supported range");
		
		return findAll(null, limit);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> findAll(Sort sort, long limit) {
		Preconditions.checkArgument( (limit>0 && limit<=DEFAULT_QUERY_LIMIT), "limit out of supported range");

		return getJdbcTemplate().query(
				sqlGrammar.selectById(tableDefinition, sort, limit, -1, rowColumnMapper.mapDynamicColumnsType()), 
				rowColumnMapper);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T find (ID id) {
		Preconditions.checkNotNull(id, "id must be provided");
		
		final Object[] idColumns = (id instanceof Object[]) ? (Object[]) id : new Object[]{id};
		final List<T> entity = jdbcTemplate.query(
				sqlGrammar.selectById(tableDefinition, null, DEFAULT_QUERY_LIMIT, 1, rowColumnMapper.mapDynamicColumnsType()), 
				rowColumnMapper,
				idColumns
				);
		return entity.isEmpty() ? null : entity.get(0);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find (Iterable<ID> ids) {
		Preconditions.checkNotNull(ids, "ids must be provided");
		
		return find(ids, null, DEFAULT_QUERY_LIMIT);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find (Iterable<ID> ids, Sort sort) {
		Preconditions.checkNotNull(ids, "ids must be provided");
		Preconditions.checkNotNull(sort, "sort must be provided");
		
		return find(ids, sort, DEFAULT_QUERY_LIMIT);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find (final Iterable<ID> ids, long limit) {
		Preconditions.checkNotNull(ids, "ids must be provided");
		Preconditions.checkArgument( (limit>0 && limit<=DEFAULT_QUERY_LIMIT), "limit out of supported range");
		
		return find(ids, null, limit);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find (final Iterable<ID> ids, Sort sort, long limit) {
		Preconditions.checkNotNull(ids, "ids must be provided");
		Preconditions.checkArgument( (limit>0 && limit<=DEFAULT_QUERY_LIMIT), "limit out of supported range");
		
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
		
		return getJdbcTemplate().query(
				sqlGrammar.selectById(tableDefinition, sort, limit, idsList.size(), rowColumnMapper.mapDynamicColumnsType()), 
				rowColumnMapper, 
				idColumnValuesList.toArray());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find (final List<Condition> conditions) {
		Preconditions.checkNotNull(conditions, "conditions must be provided");
		
		return find(conditions, null, DEFAULT_QUERY_LIMIT);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find (final List<Condition> conditions, long limit) {
		Preconditions.checkNotNull(conditions, "conditions must be provided");
		Preconditions.checkArgument( (limit>0 && limit<DEFAULT_QUERY_LIMIT), "limit out of supported range");
		
		return find(conditions, null, limit);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find (final List<Condition> conditions, Sort sort) {
		Preconditions.checkNotNull(conditions, "conditions must be provided");
		Preconditions.checkNotNull(sort, "sort must be provided");
		
		return find(conditions, sort, DEFAULT_QUERY_LIMIT);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find (final List<Condition> conditions, Sort sort, long limit) {
		Preconditions.checkNotNull(conditions, "conditions must be provided");
		Preconditions.checkArgument( (limit>0 && limit<=DEFAULT_QUERY_LIMIT), "limit out of range");

		return getJdbcTemplate().query(
				sqlGrammar.selectByCondition(tableDefinition, sort, limit, conditions, rowColumnMapper.mapDynamicColumnsType()), 
				rowColumnMapper, 
				Condition.getParamsFromConditions(conditions));
	}
	
	private void transactionalUpdate (String preparedStatement, Object... args) {
		TransactionDefinition def = new DefaultTransactionDefinition();
		TransactionStatus status = transactionManager.getTransaction(def);
		try{
			getJdbcTemplate().update(preparedStatement, args);
			transactionManager.commit(status);
		} catch (DataAccessException e) {
			LOG.error("Error in upserting record, rolling back");
			transactionManager.rollback(status);
			LOG.error(Throwables.getStackTraceAsString(e));
		}
	}
	
	private void transactionalBatchUpdate (String preparedStatement, List<Object[]> args) {
		TransactionDefinition def = new DefaultTransactionDefinition();
		TransactionStatus status = transactionManager.getTransaction(def);
		try{
			getJdbcTemplate().batchUpdate(preparedStatement, args);
			transactionManager.commit(status);
		} catch (DataAccessException e) {
			LOG.error("Error in upserting records, rolling back");
			transactionManager.rollback(status);
			LOG.error(Throwables.getStackTraceAsString(e));
		}
	}

}
