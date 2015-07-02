package org.cg.rooster.phoenix;

import java.io.Serializable;

import org.cg.rooster.JdbcDataRepository;
import org.cg.rooster.core.DynamicRowColumnMapper;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.TableDefinition;
import org.springframework.data.domain.Persistable;

/**
 * A JDBC data repository using Phoenix JDBC driver
 * @author WZ
 *
 * @param <T>
 * @param <ID>
 */
public class PhoenixJdbcDataRepository <T extends Persistable<ID>, ID extends Serializable> extends JdbcDataRepository<T, ID> {

	public PhoenixJdbcDataRepository(TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper) {
		super(tableDefinition, rowColumnMapper);
	}
	
	public PhoenixJdbcDataRepository(TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper, DynamicRowColumnMapper<T> dynamicRowColumnMapper) {
		super(tableDefinition, rowColumnMapper, dynamicRowColumnMapper);
	}
	
	@Override
	public void init () {
		this.setSqlGrammar( new PhoenixSqlGrammar() );
		this.setDataSource( new PhoenixDataSource(this.getTableDefinition().getTenantId()) );
	}

}
