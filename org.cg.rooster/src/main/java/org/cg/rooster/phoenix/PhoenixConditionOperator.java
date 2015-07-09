package org.cg.rooster.phoenix;

import org.cg.rooster.core.ConditionOperator;

/**
 * A Enum implements {@link ConditionOperator} that is supported by Apache Phoenix
 * @author WZ
 *
 */
public enum PhoenixConditionOperator implements ConditionOperator {
	
	EQUAL ("="),
    LESS ("<"),
    GREATER (">"),
	LESS_OR_EQUAL ("<="),
    GREATER_OR_EQUAL (">="),
    NOT_EQUAL ("!="),
    LIKE ("LIKE"),
    ILIKE ("ILIKE"),
    IS_NOT_NULL ("IS NOT NULL"),
    IS_NULL ("IS NULL"),
    AND("AND"),
    OR("OR");
    // BETWEEN_AND ("BETWEEN;AND"); not supported for now can be constructed using above operators
	
    private final String operator;       

    private PhoenixConditionOperator(String s) {
    	operator = s;
    }

    public boolean equals(String operator){
        return (operator == null)? false:operator.equals(operator);
    }

    public String toString(){
       return operator;
    }

	@Override
	public String getOperatorSQLStr() {
		return operator;
	}
}
