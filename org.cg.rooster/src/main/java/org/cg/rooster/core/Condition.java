package org.cg.rooster.core;

import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Defines a condition that can be used for where clause
 * @author WZ
 *
 */
public class Condition {
	
	private final Object lhsOperand;
	private final Object rhsOperand;
	private final ConditionOperator operator;
	
	public final static String SPACE = " ";
	public final static String PLACEHOLDER = " ?";
	
	/**
	 * Construct a Condition for where clause. Any column name/type mismatch will cause a SQLException during query
	 * @param columnName The column name
	 * @param operator The {@link ConditionOperator}
	 * @param value The condition value
	 */
	public Condition (String columnName, ConditionOperator operator, Object value) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(columnName), "operator must be provided");
		Preconditions.checkNotNull(operator, "operator must be provided");
		
		this.lhsOperand = columnName;
		this.operator = operator;
		this.rhsOperand = value;
	}
	
	/**
	 * Construct a Condition for where clause. Any column name/type mismatch will cause a SQLException during query
	 * @param c1 The condition
	 * @param operator The {@link ConditionOperator}
	 * @param c2 The condition
	 */
	public Condition (Condition c1, ConditionOperator operator, Condition c2) {
		Preconditions.checkNotNull(c1, "c1 must be provided");
		Preconditions.checkNotNull(operator, "operator must be provided");
		Preconditions.checkNotNull(c2, "c2 must be provided");

		this.lhsOperand = c1;
		this.operator = operator;
		this.rhsOperand = c2;
	}
	
	public Object getLhsOperand() {
		return lhsOperand;
	}
	
	public Object getRhsOperand() {
		return rhsOperand;
	}
	
	public ConditionOperator getOperator() {
		return operator;
	}
	
	/**
	 * Recursively parse conditions and generates the SQL for PreparedStatement
	 * @param conditon The condition
	 * @return
	 */
	public static String parseCondition (Condition conditon) {
		Preconditions.checkNotNull(conditon, "condition must be provided");
		
		Object lhs = conditon.getLhsOperand();
		Object rhs = conditon.getRhsOperand();
		String op = conditon.getOperator().getOperatorSQLStr();
		if (lhs instanceof String) {
			final StringBuilder sb = new StringBuilder();
			sb.append("(");
			sb.append((String)lhs).append(SPACE).append(op);
			if (rhs!=null) {
				sb.append(PLACEHOLDER);
			}
			sb.append(")");
			return sb.toString();
		} else if (lhs instanceof Condition && rhs instanceof Condition) {
			return parseCondition((Condition)lhs).concat(SPACE).concat(op).concat(SPACE).concat(parseCondition((Condition)rhs));
		} else {
		    throw new UnsupportedOperationException("Invalid operation for condition.");
		}
	}
	
	/**
	 * Recursively parse parameters and generates the parameter lists for PreparedStatement
	 * @param conditions The condition list
	 * @return
	 */
	public static Object[] getParamsFromConditions (final List<Condition> conditions) {
		Preconditions.checkNotNull(conditions, "conditions must be provided");
		
		final List<Object> params = new LinkedList<Object>();
		for (Condition c : conditions) {
			parseConditionParam(c, params);
		}
		return params.toArray();
	}
	
	private static void parseConditionParam (Condition c, List<Object> params) {
		Object lhs = c.getLhsOperand();
		Object rhs = c.getRhsOperand();		
		if (lhs instanceof String) {
			if (rhs!=null) params.add(rhs);
			return;
		} else if (lhs instanceof Condition && rhs instanceof Condition) {
			parseConditionParam((Condition)lhs, params);
			parseConditionParam((Condition)rhs, params);
		} else {
		    throw new UnsupportedOperationException("Invalid operation for condition.");
		}
	}

}
