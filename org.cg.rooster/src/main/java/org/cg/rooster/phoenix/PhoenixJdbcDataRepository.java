package org.cg.rooster.phoenix;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.rooster.JdbcDataRepository;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.TableDefinition;
import org.springframework.data.domain.Persistable;

import com.google.common.base.Throwables;

/**
 * A extended {@link JdbcDataRepository} using Apache Phoenix JDBC driver and SQL grammar
 * @author WZ
 *
 * @param <T>
 * @param <ID>
 */
public class PhoenixJdbcDataRepository <T extends Persistable<ID>, ID extends Serializable> extends JdbcDataRepository<T, ID> {

	private static final Log LOG = LogFactory.getLog(PhoenixJdbcDataRepository.class);

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
		//try init one connection in the pool
		Connection connection = null;
		try {
			connection = this.getJdbcTemplate().getDataSource().getConnection();
		} catch (SQLException e) {
			LOG.error(Throwables.getStackTraceAsString(e));
		} finally {
			if (connection!=null) {
				try {
					connection.close();
				} catch (SQLException e) {
					LOG.error(Throwables.getStackTraceAsString(e));
				}
			}
		}
	}
}
