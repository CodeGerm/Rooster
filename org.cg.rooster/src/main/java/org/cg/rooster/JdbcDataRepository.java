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
import org.cg.rooster.core.Query;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.SqlGrammar;
import org.cg.rooster.core.TableDefinition;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Persistable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

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
	 * @param dataSource the data source
	 * @param sqlGrammar the sql grammar
	 */
	public JdbcDataRepository (TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper, DataSource dataSource, SqlGrammar sqlGrammar) {
		this(tableDefinition, rowColumnMapper, dataSource, sqlGrammar, true);
	}
	
	/**
	 * Constructor
	 * 
	 * @param tableDefinition the table definition
	 * @param rowColumnMapper the row column mapper 
	 * @param dataSource the data source
	 * @param sqlGrammar the sql grammar
	 * @param lazyinit  is lazy connection initialization or not
	 */
	public JdbcDataRepository (TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper, DataSource dataSource, SqlGrammar sqlGrammar, boolean lazyinit) {
		super();
		Preconditions.checkNotNull(tableDefinition, "tableDefinition must be provided");
		Preconditions.checkNotNull(rowColumnMapper, "rowColumnMapper must be provided");
		Preconditions.checkNotNull(dataSource, "dataSource must be provided");
		Preconditions.checkNotNull(sqlGrammar, "sqlGrammar must be provided");
		
		this.tableDefinition = tableDefinition;
		this.rowColumnMapper = rowColumnMapper;
		this.jdbcTemplate = new JdbcTemplate(dataSource, lazyinit);
		this.jdbcTemplate.setFetchSize(1000);
		this.sqlGrammar = sqlGrammar;
		this.transactionManager = new DataSourceTransactionManager(dataSource);
	}
	
	protected JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
	
	protected TableDefinition getTableDefinition() {
		return tableDefinition;
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
		Preconditions.checkState(!tableDefinition.isReadonly(), "table is readonly");

		final Map<String, Object> columns = rowColumnMapper.mapColumns(entity);		
		final Map<String, Object> dynamicColumns = rowColumnMapper.mapDynamicColumns(entity);
		
		Preconditions.checkState(columns!=null && !columns.isEmpty(), "rowColumnMapper.mapColumns must be implemented");
		Preconditions.checkState(dynamicColumns!=null, "rowColumnMapper.mapDynamicColumns cannot cannot return null");
				
		this.transactionalUpdate(
				sqlGrammar.save(tableDefinition, columns, dynamicColumns), 
				ArrayUtils.addAll(columns.values().toArray(), dynamicColumns.values().toArray()));	
		
		LOG.info(String.format("[save]entity saved: %s.", entity));
		return entity;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <S extends T> Iterable<S> save (final Iterable<S> entities) {
		Preconditions.checkNotNull(entities, "entities must be provided");
		Preconditions.checkState(rowColumnMapper!=null, "rowColumnMapper must be initiated");
		Preconditions.checkState(!tableDefinition.isReadonly(), "table is readonly");
		
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
			Preconditions.checkState(columns!=null && !columns.isEmpty(), "rowColumnMapper.mapColumns must be implemented");
			Preconditions.checkState(dynamicColumns!=null, "rowColumnMapper.mapDynamicColumns cannot cannot return null");
			batchArgs.add(ArrayUtils.addAll(columns.values().toArray(), dynamicColumns.values().toArray()));
			if (!iter.hasNext()) {
				// In order to get column mapping for query construction, 
				// we only need to get the mapping from the last entity.
				createQuery = sqlGrammar.save(tableDefinition, columns, dynamicColumns);
			}
		}
		long start = System.currentTimeMillis();
		this.transactionalBatchUpdate(createQuery, batchArgs);
		long end = System.currentTimeMillis() - start;
		LOG.info(String.format("[save]saved %s entities in %sms", batchArgs.size(), end));
		return entities;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean exists (ID id) {
		Preconditions.checkNotNull(id, "id must be provided");

		return this.get(id) != null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long count() {
		LOG.info(String.format("[count]%s", tableDefinition.getTableName()));
		return getJdbcTemplate().queryForObject(sqlGrammar.count(tableDefinition), Long.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete (ID id) {
		Preconditions.checkNotNull(id, "id must be provided");
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");
		Preconditions.checkState(!tableDefinition.isReadonly(), "table is readonly");

		final Object[] idColumns = (id instanceof Object[]) ? (Object[]) id : new Object[]{id};
		this.transactionalUpdate(sqlGrammar.delete(tableDefinition), idColumns);
		LOG.info(String.format("[delete]deleted %s", id));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete (final Iterable<ID> ids) {
		Preconditions.checkNotNull(ids, "ids must be provided");
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");
		Preconditions.checkState(!tableDefinition.isReadonly(), "table is readonly");

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
		LOG.info(String.format("[delete]%s entities deleted", batchArgs.size()));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T get (ID id) {
		final Object[] idColumns = (id instanceof Object[]) ? (Object[]) id : new Object[]{id};
		LOG.info(String.format("[get]id:%s", Arrays.toString(idColumns)));
		
		long start = System.currentTimeMillis();
		final List<T> entity = jdbcTemplate.query(
				sqlGrammar.selectById(
						tableDefinition, 
						null, 
						Query.DEFAULT_QUERY_LIMIT, 
						1, 
						rowColumnMapper.mapDynamicColumnsType(), 
						null), 
				rowColumnMapper,
				idColumns
				);
		long end = System.currentTimeMillis() - start;
		LOG.info(String.format("[get]found in %sms", end));
		return entity.isEmpty() ? null : entity.get(0);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> findAll () {
		long start = System.currentTimeMillis();
		List<T> result = getJdbcTemplate().query(
				sqlGrammar.selectById(
						tableDefinition, 
						null, 
						Query.DEFAULT_QUERY_LIMIT,
						-1, 
						rowColumnMapper.mapDynamicColumnsType(), 
						null), 
				rowColumnMapper);
		long end = System.currentTimeMillis() - start;
		LOG.info(String.format("[findAll]limit:%s. found %s in %sms", Query.DEFAULT_QUERY_LIMIT, result.size(), end));
		return result;
	}
		
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find(Query query) {
		Preconditions.checkNotNull(query, "query must be provided");

		List<T> result;
		long start = System.currentTimeMillis();
		if (query.getConditions()==null || query.getConditions().isEmpty()) { 
			result = getJdbcTemplate().query(
					sqlGrammar.selectById(
							tableDefinition, 
							query.getSort(), 
							query.getLimit(), 
							-1, 
							rowColumnMapper.mapDynamicColumnsType(), 
							query.getColumnSelection()), 
					rowColumnMapper);
		} else {
			result = getJdbcTemplate().query(
					sqlGrammar.selectByCondition(
							tableDefinition, 
							query.getSort(), 
							query.getLimit(), 
							query.getConditions(), 
							rowColumnMapper.mapDynamicColumnsType(), 
							query.getColumnSelection()), 
					rowColumnMapper, 
					Condition.getParamsFromConditions(query.getConditions()));	
		}
		long end = System.currentTimeMillis() - start;
		LOG.info(String.format("[find]query: %s in %sms", query, end));
		return result;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find(Iterable<ID> ids) {
		return this.find(ids, new Query(null, null, null, Query.DEFAULT_QUERY_LIMIT));
	}
			
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterable<T> find(Iterable<ID> ids, Query query) {
		Preconditions.checkNotNull(ids, "ids must be provided");
		Preconditions.checkNotNull(query, "query must be provided");		
		if (query.getConditions()!=null && !query.getConditions().isEmpty()) {
			LOG.warn("[find]conditions will be ignored when lookup by id list");
		}
		if (!ids.iterator().hasNext()) {
			return Collections.emptyList();
		}
		
		//need to put all id components for all ids in a single flat array
		final List<Object> idColumnValuesList = new LinkedList<Object>();
		int idSize = 0;
		for (ID id : ids) {
			List<Object> idList;
			if (id instanceof Object[]) {
				idList = Arrays.asList((Object[]) id);
			} else {
				idList = Collections.<Object>singletonList(id);
			}
			idColumnValuesList.addAll(idList);
			idSize++;
		}
		
		Object[] idsArray = idColumnValuesList.toArray();		
		long start = System.currentTimeMillis();
		List<T> result = getJdbcTemplate().query(
				sqlGrammar.selectById(
						tableDefinition, 
						query.getSort(), 
						query.getLimit(), 
						idSize, 
						rowColumnMapper.mapDynamicColumnsType(), 
						query.getColumnSelection()), 
				rowColumnMapper, 
				idsArray);
		long end = System.currentTimeMillis() - start;
		LOG.info(String.format("[find]ids:%s; query:%s; found %s in %sms", 
				Arrays.toString(idsArray), query, result.size(), end));
		return result;
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
