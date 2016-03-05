package org.cg.rooster.core;

import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Sort;

/**
 * A interface for sql grammar
 * @author WZ
 *
 */
public interface SqlGrammar {

	/**
	 * get the SQL dialect supported data type from the argument
	 * 
	 * @param arg The argument Object
	 * @return the SQL string
	 */
	public String getParamDataType (Object arg);

	/**
	 * generate SQL for counting rows
	 * 
	 * @param table the {@link TableDefinition} class
	 * @return the SQL string
	 */
	public String count (TableDefinition table);

	/**
	 * generate SQL for deleting row(s)
	 * 
	 * @param table the {@link TableDefinition} class
	 * @return the SQL string
	 */

	public String delete (TableDefinition table);
	
	/**
	 * generate SQL for selecting row(s) by id
	 *  
	 * @param table table the {@link TableDefinition} class
	 * @param sort
	 * @param limit
	 * @param idSize
	 * @param dynamicColumnsType
	 * @param columnSelection 
	 * @return the SQL string
	 */
	public String selectById (TableDefinition table, Sort sort, long limit, int idSize, 
			final Map<String, String> dynamicColumnsType, 
			final List<String> columnSelection);
	
	/**
	 * generate SQL for selecting row(s) with conditions
	 *  
	 * @param table the {@link TableDefinition} class
	 * @param sort
	 * @param limit
	 * @param conditions
	 * @param dynamicColumnsType
	 * @param columnSelection
	 * @return the SQL string
	 */
	public String selectByCondition (TableDefinition table, Sort sort, long limit, 
			final List<Condition> conditions,  
			final Map<String, String> dynamicColumnsType, 
			final List<String> columnSelection);
	
	/**
	 * generate SQL for saving row(s) 
	 * 
	 * @param table
	 * @param columnMapper The column mapper
	 * @param dynamicColumnMapper The dynamic column mapper
	 * @return the SQL string
	 */
	public String save (TableDefinition table, final Map<String, Object> columnMapper, 
			final Map<String, Object> dynamicColumnMapper);

}
