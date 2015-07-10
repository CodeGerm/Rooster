package org.cg.rooster.phoenix;

import java.io.Serializable;

import org.cg.rooster.JdbcDataRepository;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.TableDefinition;
import org.springframework.data.domain.Persistable;

/**
 * A extended {@link JdbcDataRepository} using Apache Phoenix JDBC driver and SQL grammar
 * @author WZ
 *
 * @param <T>
 * @param <ID>
 */
public class PhoenixJdbcDataRepository <T extends Persistable<ID>, ID extends Serializable> extends JdbcDataRepository<T, ID> {

	/**
	 * 
	 * @param dataSource 
	 * @param tableDefinition
	 * @param rowColumnMapper
	 */
	public PhoenixJdbcDataRepository(PhoenixDataSource dataSource, TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper) {
		super(tableDefinition, 
			  rowColumnMapper, 
			  dataSource,
			  PhoenixSqlGrammar.getInstance());
	}
}
