package org.cg.rooster.core;

import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

/**
 * Interface of row column mapper for dynamic columns
 * @author WZ
 *
 * @param <T>
 */
public interface DynamicRowColumnMapper<T> extends RowMapper<T> {

	public Map<String, Object> mapColumns(T t);

}
