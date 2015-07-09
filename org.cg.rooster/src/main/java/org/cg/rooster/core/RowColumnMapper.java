package org.cg.rooster.core;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

/**
 * A abstract class that implements {@link RowMapper} used to map rows
 * 
 * @author WZ
 *
 * @param <T> The entity type
 */
public abstract class RowColumnMapper<T> implements RowMapper<T> {
	
	/**
	 * 
	 * Implementations must implement this method to map each fields of data in the T in order to persist the entity. 
	 * 
	 * @param t The entity to be mapped
	 * @return A Map that maps each fields of data to its column names in table
	 */
	public abstract Map<String, Object> mapColumns(T t);
	
	/**
	 * 
	 * Implementations can implement this method to map fields of data in the T 
	 * that is dynamic columns in table in order to persist the entity. 
	 * 
	 * ALSO IMPLEMENT {@link RowColumnMapper}.mapDynamicColumnsType 
	 * 
	 * @param t The entity to be mapped
	 * @return A Map that maps fields of data to its dynamic column names in table
	 */
	public Map<String, Object> mapDynamicColumns(T t) {
		return new LinkedHashMap<String, Object>();
	}
	
	/**
	 * 
	 * Implementations can implement this method to map each column to its data type in the table. 
	 * 
	 * ALSO IMPLEMENT {@link RowColumnMapper}.mapDynamicColumnsType 
	 * 
	 * @return A Map that maps the column name and it's data type
	 */
	public Map<String, String> mapDynamicColumnsType() {
		return new LinkedHashMap<String, String>();
	}

}
