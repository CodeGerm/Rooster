package org.cg.rooster.core;

/**
 * Implement this interface with an enum to support specific SQL dialect
 * @author WZ
 *
 */
public interface ConditionOperator {
	
	/**
	 * Get the operator SQL string
	 * @return the operator SQL string
	 */
	public String getOperatorSQLStr();
}
