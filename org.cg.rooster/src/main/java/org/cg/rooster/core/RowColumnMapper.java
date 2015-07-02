package org.cg.rooster.core;

import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

/**
 * Interface of row column mapper for normal columns
 * @author WZ
 *
 * @param <T>
 */
public interface RowColumnMapper <T> extends RowMapper<T> {

	public Map<String, Object> mapColumns(T t);

}
