package org.cg.rooster.phoenix;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.Properties;

import org.apache.hadoop.hbase.util.Strings;

import com.google.common.base.Preconditions;

/**
 * Extends a {@link DriverManagerDataSource} with settings that Apache Phoenix supports
 * @author WZ
 *
 */
public class PhoenixDataSource extends DriverManagerDataSource {

	protected String phoenixDriverClassName; 
	protected String phoenixConnectionUrl;
	protected Integer upsertBatchSize;
	protected Boolean autocommit;
	protected String tenantId;

	private static final Integer DEFAULT_UPSERT_BATCH_SIZE = 1000;
	private static final String TENANT_ID_ATTRIB = "TenantId";
	private static final String UPSERT_BATCH_SIZE_ATTRIB = "UpsertBatchSize";
	private static final String AUTO_COMMIT_ATTRIB = "AutoCommit";
    
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
		this(phoenixDriverClassName, phoenixConnectionUrl, true);
	}
	
	/**
	 * 
	 * @param phoenixDriverClassName The JDBC driver class name
	 * @param phoenixConnectionUrl The connection url
	 * @param autocommit auto commit or not
	 */
	public PhoenixDataSource(
			String phoenixDriverClassName, 
			String phoenixConnectionUrl,
			Boolean autocommit) {
		this(phoenixDriverClassName, phoenixConnectionUrl, autocommit, DEFAULT_UPSERT_BATCH_SIZE);
	}
	
	/**
	 * 
	 * @param phoenixDriverClassName The JDBC driver class name
	 * @param phoenixConnectionUrl The connection url
	 * @param autocommit Disable auto commit if you want batch update
	 * @param upsertBatchSize Only used when autoCommit is true
	 */
	public PhoenixDataSource(
			String phoenixDriverClassName, 
			String phoenixConnectionUrl, 
			Boolean autocommit,
			Integer upsertBatchSize) {
		this(phoenixDriverClassName, phoenixConnectionUrl, autocommit, upsertBatchSize, null);
	}
	
	/**
	 * 
	 * @param phoenixDriverClassName The JDBC driver class name
	 * @param phoenixConnectionUrl The connection url
	 * @param autocommit Disable auto commit if you want batch update
	 * @param upsertBatchSize Only used when autoCommit is true
	 * @param tenantId The tenant Id Note: DDL property has to be enabled: http://phoenix.apache.org/multi-tenancy.html
	 */
	public PhoenixDataSource(
			String phoenixDriverClassName,
			String phoenixConnectionUrl, 
			Boolean autocommit,
			Integer upsertBatchSize,
			String tenantId) {
		super();
		setPhoenixDriverClassName(phoenixDriverClassName);
		setPhoenixConnectionUrl(phoenixConnectionUrl);
		setAutocommit(autocommit);
		setUpsertBatchSize(upsertBatchSize);
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

	public Boolean isAutocommit() {
		return autocommit;
	}

	public void setAutocommit(Boolean autocommit) {
		Preconditions.checkNotNull(autocommit, "autocommit must be provided");
		this.autocommit = autocommit;
		this.setConnectionProperty(AUTO_COMMIT_ATTRIB, autocommit.toString());
	}	
	
	public Integer getUpsertBatchSize() {
		return upsertBatchSize;
	}

	public void setUpsertBatchSize(Integer upsertBatchSize) {
		Preconditions.checkArgument(upsertBatchSize!=null&&upsertBatchSize>0, "upsertBatchSize must be valid");
		this.upsertBatchSize = upsertBatchSize;
		Integer batchSize = upsertBatchSize != null ? upsertBatchSize : DEFAULT_UPSERT_BATCH_SIZE;
		if (this.autocommit) {
			this.setConnectionProperty(UPSERT_BATCH_SIZE_ATTRIB, batchSize.toString());
		}
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		if (!Strings.isEmpty(tenantId)) {
		  this.tenantId = tenantId;
	      this.setConnectionProperty(TENANT_ID_ATTRIB, tenantId);
		}
	}
	
	private void setConnectionProperty(String key, String value) {
		Preconditions.checkArgument(!Strings.isEmpty(key), "key must be provided");
		Preconditions.checkArgument(!Strings.isEmpty(value), "value must be provided");
		Properties connectionProperties = this.getConnectionProperties();
		if (connectionProperties == null) {
			connectionProperties = new Properties();
		}
		connectionProperties.setProperty(key, value);
		this.setConnectionProperties(connectionProperties);
	}

}