package org.cg.rooster.phoenix;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.hbase.util.Strings;

import com.google.common.base.Preconditions;

/**
 * Extends a {@link BasicDataSource} with settings that Apache Phoenix supports
 * @author WZ
 *
 */
public class PhoenixDataSource extends BasicDataSource {

	protected String phoenixDriverClassName; 
	protected String phoenixConnectionUrl;
	protected Integer initialConnectionSize;
	protected Integer maxConnectionSize;
	protected Boolean autocommit;
	protected Integer tenantId;

	public PhoenixDataSource() {
		
	}
	
	/**
	 * 
	 * @param phoenixDriverClassName The JDBC driver class name
	 * @param phoenixConnectionUrl The connection url
	 */
	public PhoenixDataSource(
			String phoenixDriverClassName, 
			String phoenixConnectionUrl) {
		super();
		setPhoenixDriverClassName(phoenixDriverClassName);
		setPhoenixConnectionUrl(phoenixConnectionUrl);
	}
	
	/**
	 * 
	 * @param phoenixDriverClassName The JDBC driver class name
	 * @param phoenixConnectionUrl The connection url
	 * @param initialConnectionSize The initial connection size
	 * @param maxConnectionSize The maxim connection size
	 * @param autocommit Disable auto commit if you want batch update
	 */
	public PhoenixDataSource(
			String phoenixDriverClassName, 
			String phoenixConnectionUrl, 
			Integer initialConnectionSize,
			Integer maxConnectionSize, 
			Boolean autocommit) {
		super();
		setPhoenixDriverClassName(phoenixDriverClassName);
		setPhoenixConnectionUrl(phoenixConnectionUrl);
		setInitialConnectionSize(initialConnectionSize);
		setMaxConnectionSize(maxConnectionSize);
		setAutocommit(autocommit);
	}
	
	/**
	 * 
	 * @param phoenixDriverClassName The JDBC driver class name
	 * @param phoenixConnectionUrl The connection url
	 * @param initialConnectionSize The initial connection size
	 * @param maxConnectionSize The maxim connection size
	 * @param autocommit Disable auto commit if you want batch update
	 * @param tenantId The tenant Id Note: DDL property has to be enabled: http://phoenix.apache.org/multi-tenancy.html
	 */
	public PhoenixDataSource(
			String phoenixDriverClassName,
			String phoenixConnectionUrl, 
			Integer initialConnectionSize,
			Integer maxConnectionSize, 
			Boolean autocommit, 
			Integer tenantId) {
		super();
		setPhoenixDriverClassName(phoenixDriverClassName);
		setPhoenixConnectionUrl(phoenixConnectionUrl);
		setInitialConnectionSize(initialConnectionSize);
		setMaxConnectionSize(maxConnectionSize);
		setAutocommit(autocommit);
		setTenantId(tenantId);
	}

	public String getPhoenixDriverClassName() {
		return phoenixDriverClassName;
	}

	public void setPhoenixDriverClassName(String phoenixDriverClassName) {
		Preconditions.checkArgument(!Strings.isEmpty(phoenixDriverClassName), "phoenixDriverClassName must be provided");
		this.phoenixDriverClassName = phoenixDriverClassName;
		this.setDriverClassName(phoenixDriverClassName);
	}

	public String getPhoenixConnectionUrl() {
		return phoenixConnectionUrl;
	}

	public void setPhoenixConnectionUrl(String phoenixConnectionUrl) {
		Preconditions.checkArgument(!Strings.isEmpty(phoenixConnectionUrl), "phoenixConnectionUrl must be provided");
		this.phoenixConnectionUrl = phoenixConnectionUrl;
		this.setUrl(phoenixConnectionUrl);
	}

	public Integer getInitialConnectionSize() {
		return initialConnectionSize;
	}

	public void setInitialConnectionSize(Integer initialConnectionSize) {
		Preconditions.checkNotNull(initialConnectionSize, "initialConnectionSize must be provided");
		this.initialConnectionSize = initialConnectionSize;
		this.setInitialSize(initialConnectionSize);
	}

	public Integer getMaxConnectionSize() {
		return maxConnectionSize;
	}

	public void setMaxConnectionSize(Integer maxConnectionSize) {
		Preconditions.checkNotNull(maxConnectionSize, "maxConnectionSize must be provided");
		this.maxConnectionSize = maxConnectionSize;
		this.setMaxActive(maxConnectionSize);
	}

	public Boolean isAutocommit() {
		return autocommit;
	}

	public void setAutocommit(Boolean autocommit) {
		Preconditions.checkNotNull(autocommit, "autocommit must be provided");
		this.autocommit = autocommit;
		this.setDefaultAutoCommit(autocommit);
	}

	public Integer getTenantId() {
		return tenantId;
	}

	public void setTenantId(Integer tenantId) {
		Preconditions.checkNotNull(tenantId, "tenantId must be provided");
		this.tenantId = tenantId;
		final String tenantIdProperty = String.format("TenantId=%s;", tenantId);
		this.setConnectionProperties(tenantIdProperty);
	}

}