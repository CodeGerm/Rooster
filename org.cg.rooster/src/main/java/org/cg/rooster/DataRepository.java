package org.cg.rooster;

import java.io.Serializable;
import java.util.Map;

import org.springframework.data.domain.Persistable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.CrudRepository;

public interface DataRepository<T extends Persistable<ID>, ID extends Serializable> extends CrudRepository<T, ID> {
	
	/**
	 * Inherited operations:
	 * 
	 * T save(T entity);
	 * Iterable<T> save(Iterable<? extends T> entities);
     
     * boolean exists(ID id);
     * long count();
     
     * void delete(ID id);
     * void delete(T entity);
     * void delete(Iterable<? extends T> entities);
     * void deleteAll();
     
     * T findOne(ID id);
     * Iterable<T> findAll();
     * Iterable<T> findAll(Iterable<ID> ids);
	 * 
	 */
	
	public Iterable<T> findAll(Sort sort);
	
	public Iterable<T> findAll(long limit);
	
	public Iterable<T> findAll(Sort sort, long limit);
	
	public Iterable<T> find (final Map<String, Object> valueMapping);
	
	public Iterable<T> find (final Map<String, Object> valueMapping, long limit);
	
	public Iterable<T> find (final Map<String, Object> valueMapping, Sort sort);
	
	public Iterable<T> find (final Map<String, Object> valueMapping, Sort sort, long limit);

	
}
