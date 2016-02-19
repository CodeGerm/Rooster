package org.cg.rooster.redshift;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.cg.rooster.core.RowColumnMapper;
import org.cg.rooster.core.TableDefinition;

/**
 * A Data Repository for {@link User}
 * @author WZ
 *
 */
public class UserDataRepository extends RedshiftJdbcDataRepository<User, Object[]> {
	
	public UserDataRepository() {
		super( dataSource, tableDef, ROW_COLUMN_MAPPER );
	}

	private final static RedshiftDataSource dataSource = new RedshiftDataSource(
			"com.amazon.redshift.jdbc41.Driver", 
			"jdbc:redshift://replace_me", 
			"replace_me",
			"replace_me");
	
	private final static TableDefinition tableDef = new TableDefinition(true, "demo.user", "id");
	
	public static final RowColumnMapper<User> ROW_COLUMN_MAPPER = new RowColumnMapper<User>() {
		@Override
		public User mapRow(ResultSet rs, int rowNum) throws SQLException {
			User user = new User();
			user.setId(rs.getString("ID"));
			user.setUserName(rs.getString("Username"));
			user.setDisplayName(rs.getString("DisplayName"));
			user.setEmail(rs.getString("Email"));
			user.setLastLogin(rs.getDate("LastLogin"));
			user.setLastInvite(rs.getDate("LastInvite"));
			user.setStatus(rs.getString("Status"));
			user.setStatusEnum(rs.getString("StatusEnum"));
			user.setSourceDs(rs.getString("SourceDs"));
			user.setSourceDsType(rs.getString("SourceDsType"));
			user.setForest(rs.getString("Forest"));
			user.setDirectoryServiceUuidl(rs.getString("DirectoryServiceUuid"));
			user.setSourceDsLocalized(rs.getString("SourceDsLocalized"));
			return user;
		}
	};
}
