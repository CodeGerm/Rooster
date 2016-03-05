package org.cg.rooster.core;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

/**
 * A abstract class that implements {@link RowMapper} used to map rows
 * 
 * @author WZ
 *
 * @param <T> The entity type
 */
public abstract class SelectableRowColumnMapper<T> extends RowColumnMapper<T> {
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public abstract T mapRow(ResultSet rs, int rowNum);
	
	//TODO
	//TODO
	//TODO
	public abstract Map<String, Field> mapRows() throws SQLException;
}
