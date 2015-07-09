package org.cg.rooster.phoenix;

import java.io.Serializable;

import org.cg.rooster.JdbcDataRepository;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.TableDefinition;
import org.springframework.data.domain.Persistable;

/**
 * A extended {@link JdbcDataRepository} using Aphache Phoenix JDBC driver and SQL grammar
 * @author WZ
 *
 * @param <T>
 * @param <ID>
 */
public class PhoenixJdbcDataRepository <T extends Persistable<ID>, ID extends Serializable> extends JdbcDataRepository<T, ID> {

	public PhoenixJdbcDataRepository(TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper) {
		super(tableDefinition, rowColumnMapper);
	}
	
	@Override
	public void init () {
		this.setSqlGrammar( PhoenixSqlGrammar.getInstance() );
		this.setDataSource( new PhoenixDataSource(this.getTableDefinition().getTenantId()) );
	}

}
