package org.cg.rooster.redshift;

import java.io.Serializable;

import javax.sql.DataSource;

import org.cg.rooster.JdbcDataRepository;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.TableDefinition;
import org.springframework.data.domain.Persistable;

/**
 * A extended {@link JdbcDataRepository} using Amazon Redshift JDBC driver and SQL grammar
 * @author WZ
 *
 * @param <T>
 * @param <ID>
 */
public class RedshiftJdbcDataRepository <T extends Persistable<ID>, ID extends Serializable> extends JdbcDataRepository<T, ID> {

	/**
	 * 
	 * @param dataSource 
	 * @param tableDefinition
	 * @param rowColumnMapper
	 */
	public RedshiftJdbcDataRepository(DataSource dataSource, TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper) {
		super(tableDefinition, 
			  rowColumnMapper, 
			  dataSource,
			  RedshiftSqlGrammar.getInstance());
	}
	
	/**
	 * 
	 * @param dataSource 
	 * @param tableDefinition
	 * @param rowColumnMapper
	 */
	public RedshiftJdbcDataRepository(DataSource dataSource, TableDefinition tableDefinition, RowColumnMapper<T> rowColumnMapper, boolean lazyinit) {
		super(tableDefinition, 
			  rowColumnMapper, 
			  dataSource,
			  RedshiftSqlGrammar.getInstance(),
			  lazyinit);
	}
}
