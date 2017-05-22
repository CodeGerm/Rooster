package org.cg.rooster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang.ArrayUtils;
import org.apache.directory.api.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.cg.rooster.core.Condition;
import org.cg.rooster.core.Query;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.SqlGrammar;
import org.cg.rooster.core.TableDefinition;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Persistable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.jdbc.core.JdbcTemplate;
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

	private static final Logger LOG = LoggerFactory.getLogger(JdbcDataRepository.class);

	private final TableDefinition tableDefinition;
	private final RowColumnMapper<T> rowColumnMapper;
	private final JdbcTemplate jdbcTemplate;
	private final SqlGrammar sqlGrammar;

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
	}
	
	public JdbcTemplate getJdbcTemplate() {
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
				
		boolean isSucceed = this.upsert(
				sqlGrammar.save(tableDefinition, columns, dynamicColumns), 
				ArrayUtils.addAll(columns.values().toArray(), dynamicColumns.values().toArray()));	
		if (isSucceed) {
			LOG.info(String.format("[save]entity saved: %s.", entity));
			return entity;
		} else {
			return null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <S extends T> Iterable<S> save (final Iterable<S> entities) {
		Preconditions.checkNotNull(entities, "entities must be provided");
		Preconditions.checkState(rowColumnMapper!=null, "rowColumnMapper must be initiated");
		Preconditions.checkState(!tableDefinition.isReadonly(), "table is readonly");
		
		Map<String, Object> columns;
		Map<String, Object> dynamicColumns;
		Iterator<S> iter = entities.iterator();
		S entity;
		String createQuery = null;
		List<Object[]> batchArgs = new LinkedList<Object[]>();
		while (iter.hasNext()) {
			entity = iter.next();
			columns = rowColumnMapper.mapColumns(entity);
			dynamicColumns = rowColumnMapper.mapDynamicColumns(entity);
			Preconditions.checkState(columns!=null && !columns.isEmpty(), "rowColumnMapper.mapColumns must be implemented");
			Preconditions.checkState(dynamicColumns!=null, "rowColumnMapper.mapDynamicColumns cannot cannot return null");
			batchArgs.add(ArrayUtils.addAll(columns.values().toArray(), dynamicColumns.values().toArray()));
			String currentQuery = sqlGrammar.save(tableDefinition, columns, dynamicColumns);
			Preconditions.checkState(!Strings.isEmpty(currentQuery), "inconsistent row column mapping in batch");
			if (createQuery != null) {
		      Preconditions.checkState(createQuery.equals(currentQuery), "inconsistent row column mapping in batch");
			}
			createQuery = currentQuery;
		}
		long start = System.currentTimeMillis();
		boolean isSucceed = this.upsertBatch(createQuery, batchArgs);
		long end = System.currentTimeMillis() - start;
		if (isSucceed) {
			LOG.info(String.format("[save]saved %s entities in %sms", batchArgs.size(), end));
			return entities;
		} else {
			List<S> empty = Collections.emptyList();
			return empty;
		}
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
	public boolean delete (ID id) {
		Preconditions.checkNotNull(id, "id must be provided");
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");
		Preconditions.checkState(!tableDefinition.isReadonly(), "table is readonly");

		final Object[] idColumns = (id instanceof Object[]) ? (Object[]) id : new Object[]{id};
		Preconditions.checkArgument(idColumns.length == tableDefinition.getPrimaryId().size(), "all id components must be provided ");

		boolean isSucceed = this.upsert(sqlGrammar.delete(tableDefinition, 1, idColumns), filterOutNull(idColumns));
		if (isSucceed) LOG.info(String.format("[delete]deleted %s", id));
		return isSucceed;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean delete (final Iterable<ID> ids) {
		Preconditions.checkNotNull(ids, "ids must be provided");
		Preconditions.checkArgument(ids.iterator().hasNext(), "ids must be provided");
		Preconditions.checkState(tableDefinition.isMutable(), "table is immutable");
		Preconditions.checkState(!tableDefinition.isReadonly(), "table is readonly");
		
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
		boolean isSucceed = this.upsert(sqlGrammar.delete(tableDefinition, idSize, idsArray), filterOutNull(idsArray));
		if (isSucceed) LOG.info(String.format("[delete]%s entities deleted", idSize));
		return isSucceed;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T get (ID id) {
		final Object[] idColumns = (id instanceof Object[]) ? (Object[]) id : new Object[]{id};
		Preconditions.checkArgument(idColumns.length == tableDefinition.getPrimaryId().size(), "all id components must be provided ");
		LOG.info(String.format("[get]id:%s", Arrays.toString(idColumns)));
		
		long start = System.currentTimeMillis();
		final List<T> entity = jdbcTemplate.query(
				sqlGrammar.selectById(
						tableDefinition, 
						null, 
						Query.DEFAULT_QUERY_LIMIT, 
						1,
						idColumns,
						rowColumnMapper.mapDynamicColumnsType(), 
						null), 
				rowColumnMapper,
				filterOutNull(idColumns)
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
						null,
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
							null,
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
						idsArray,
						rowColumnMapper.mapDynamicColumnsType(), 
						query.getColumnSelection()), 
				rowColumnMapper, 
				filterOutNull(idsArray));
		long end = System.currentTimeMillis() - start;
		LOG.info(String.format("[find]ids:%s; query:%s; found %s in %sms", 
				Arrays.toString(idsArray), query, result.size(), end));
		return result;
	}
	
	private boolean upsert (String preparedStatement, Object... args) {
		try{
			getJdbcTemplate().update(preparedStatement, args);
			return true;
		} catch (DataAccessException e) {
			LOG.error("Error in upserting record");
			LOG.error(Throwables.getStackTraceAsString(e));
			return false;
		}
	}
	
	private boolean upsertBatch (String preparedStatement, List<Object[]> args) {
		try{
			getJdbcTemplate().batchUpdate(preparedStatement, args);
			return true;
		} catch (DataAccessException e) {
			LOG.error("Error in upserting records");
			LOG.error(Throwables.getStackTraceAsString(e));
			return false;
		}
	}
	
	private Object[] filterOutNull (Object[] inputArray) {
		List<Object> list = new ArrayList<Object>();
	    for(Object s : inputArray) {
	       if(s!=null) list.add(s);
	    }
	    return list.toArray(new Object[list.size()]);
	}
}
