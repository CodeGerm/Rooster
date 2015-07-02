package org.cg.rooster.core;

import java.util.Set;

import org.springframework.data.domain.Sort;

/**
 * A interface for sql grammar
 * @author WZ
 *
 */
public interface SqlGrammar {

	public String count(TableDefinition table);
		
	public String delete (TableDefinition table);
	
	public String deleteAll (TableDefinition table);
			
	public String select (TableDefinition table, Sort sort, long limit, int idSize);
		
	public String save (TableDefinition table, final Set<String> columnSet);
		
}
