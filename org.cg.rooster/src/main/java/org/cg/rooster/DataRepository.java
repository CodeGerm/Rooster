package org.cg.rooster;

import java.io.Serializable;
import java.util.List;

import org.cg.rooster.core.Condition;
import org.springframework.data.domain.Persistable;
import org.springframework.data.domain.Sort;

/**
 * Interface for generic CRUD operations on a repository for a specific type. 
 * @author WZ
 *
 * @param <T>
 * @param <ID>
 */
public interface DataRepository<T extends Persistable<ID>, ID extends Serializable> {

	/**
	 * save (create/update) the entity
	 * 
	 * @param entity
	 * @return the saved entity
	 */
	public <S extends T> S save (S entity);
	
	/**
	 * save (create/update) a collection of entities
	 * 
	 * @param entities
	 * @return the saved entities
	 */
	public <S extends T> Iterable<S> save (final Iterable<S> entities);
	
	/**
	 * check if the entity exist by id
	 * 
	 * @param id
	 * @return true if the entity exists
	 */
	public boolean exists (ID id);
	
	/**
	 * get the total number of entities in the table
	 * 
	 * @return the number of rows
	 */
	public long count ();
	
	/**
	 * delete a entity by id
	 *
	 * @param id
	 */
	public void delete (ID id);
	
	/**
	 * delete a collection of entities by their ids
	 * 
	 * @param ids
	 */
	public void delete (final Iterable<ID> ids);
	    	
	/**
	 * find all entities
	 * 
	 * @return a collection of entities
	 */
	public Iterable<T> findAll ();
	
	/**
	 * find all entities and sort
	 * 
	 * @param sort the sorting order description
	 * @return a collection of entities
	 */
	public Iterable<T> findAll (Sort sort);
	
	/**
	 * find all entities with limit
	 * 
	 * @param limit
	 * @return a collection of entities
	 */
	public Iterable<T> findAll (long limit);
	
	/**
	 * find all entities with limit and sort
	 * 
	 * @param sort
	 * @param limit
	 * @return a collection of entities
	 */
	public Iterable<T> findAll (Sort sort, long limit);
	
	/**
	 * find a entity by id
	 * 
	 * @param id
	 * @return the entity
	 */
	public T find (ID id);
	
	/**
	 * find entities with a collection of ids
	 * 
	 * @param ids
	 * @return a collection of entities
	 */
	public Iterable<T> find (final Iterable<ID> ids);
	
	/**
	 * find entities with a collection of ids and sort
	 * 
	 * @param ids
	 * @param sort
	 * @return a collection of entities
	 */
	public Iterable<T> find (final Iterable<ID> ids, Sort sort);
	
	/**
	 * find entities with a collection of ids with limit
	 * 
	 * @param ids
	 * @param limit
	 * @return a collection of entities
	 */
	public Iterable<T> find (final Iterable<ID> ids, long limit);
	
	/**
	 * find entities with a collection of ids with limit and sort
	 * 
	 * @param ids
	 * @param sort
	 * @param limit
	 * @return a collection of entities
	 */
	public Iterable<T> find (final Iterable<ID> ids, Sort sort, long limit);
	
	public Iterable<T> find (final List<Condition> conditions);
	
	public Iterable<T> find (final List<Condition> conditions, long limit);
	
	public Iterable<T> find (final List<Condition> conditions, Sort sort);
	
	public Iterable<T> find (final List<Condition> conditions, Sort sort, long limit);

}
