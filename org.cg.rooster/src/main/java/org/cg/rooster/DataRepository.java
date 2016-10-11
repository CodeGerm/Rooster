package org.cg.rooster;

import java.io.Serializable;
import org.cg.rooster.core.Query;
import org.springframework.data.domain.Persistable;

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
	 * @param entity the entity to save
	 * @return the saved entity, or null if failed
	 */
	public <S extends T> S save (S entity);
	
	/**
	 * save (create/update) a collection of entities
	 * 
	 * @param entities the entities to save
	 * @return the saved entities, or empty if failed
	 */
	public <S extends T> Iterable<S> save (final Iterable<S> entities);
	
	/**
	 * check if the entity exist by id
	 * 
	 * @param id the id
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
	 * @param id the id
	 * @return if succeed
	 */
	public boolean delete (ID id);
	
	
	/**
	 * delete a collection of entities by their ids
	 * 
	 * @param ids the ids list
	 * @return if succeed
	 */
	public boolean delete (final Iterable<ID> ids);
	
	/**
	 * get a entity by id
	 * 
	 * @param id the id
	 * @return the entity
	 */
	public T get (ID id);
	    	
	/**
	 * find all entities
	 * 
	 * @return a collection of entities
	 */
	public Iterable<T> findAll ();
	
	/**
	 * find entities with query
	 * 
	 * @param query the query
	 * @return a collection of entities
	 */
	public Iterable<T> find (Query query);
	
	/**
	 * find entities with a collection of ids
	 * 
	 * @param ids the ids list
	 * @return a collection of entities
	 */
	public Iterable<T> find (final Iterable<ID> ids);
	
	/**
	 * find entities with a collection of ids and query
	 * 
	 * @param ids the ids list
	 * @param query the query
	 * @return a collection of entities
	 */
	public Iterable<T> find (final Iterable<ID> ids, Query query);
}
