package org.cg.rooster.phoenix;

import org.apache.commons.dbcp.BasicDataSource;

/**
 * Extends a {@link BasicDataSource} for Apache Phoenix
 * @author WZ
 *
 */
public class PhoenixDataSource extends BasicDataSource {

	//TODO will make them configurable
	private String phoenixDriverClassName = "org.apache.phoenix.jdbc.PhoenixDriver"; 
	private String phoenixConnectionUrl = "jdbc:phoenix:localhost";
	private int JdbcInitialConnectionSize = 20;
	private int JdbcMaxConnectionSize = 0; //non-limit

	public PhoenixDataSource () {
		this(-1);
	}
	
	/**
	 * 
	 * @param tenantId the tenantId for this repo. 
	 * Note: DDL property has to be enabled: {@link http://phoenix.apache.org/multi-tenancy.html}
	 */
	public PhoenixDataSource (int tenantId) {
		super();
		
		this.setDriverClassName(phoenixDriverClassName);
		this.setInitialSize(JdbcInitialConnectionSize);
		this.setMaxActive(JdbcMaxConnectionSize);
		this.setDefaultAutoCommit(true);
		this.setUrl(phoenixConnectionUrl);
		
		if ( tenantId>0 ) {
			final String tenantIdProperty = String.format("TenantId=%s;", tenantId);
			this.setConnectionProperties(tenantIdProperty);
		}
	}
	
}
