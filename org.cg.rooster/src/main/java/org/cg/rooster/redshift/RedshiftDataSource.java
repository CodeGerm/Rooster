package org.cg.rooster.redshift;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.hbase.util.Strings;

import com.google.common.base.Preconditions;

/**
 * Extends a {@link BasicDataSource} with settings that Amazon Redshift supports
 * @author WZ
 *
 */
public class RedshiftDataSource extends BasicDataSource {
	
	protected String redshiftDriverClassName; 
	protected String redshiftConnectionUrl;
	protected Integer initialConnectionSize;
	protected Integer maxConnectionSize;
	protected Boolean autocommit;

	public RedshiftDataSource() {

	}
	
	/**
	 * 
	 * @param redshiftDriverClassName The JDBC driver class name
	 * @param redshiftConnectionUrl The connection URL
	 */
	public RedshiftDataSource(
			String redshiftDriverClassName, 
			String redshiftConnectionUrl,
			String username,
			String password) {
		super();
		setRedshiftDriverClassName(redshiftDriverClassName);
		setRedshiftConnectionUrl(redshiftConnectionUrl);
		this.setUsername(username);
		this.setPassword(password);
	}
	
	/**
	 * 
	 * @param redshiftDriverClassName The JDBC driver class name
	 * @param redshiftConnectionUrl The connection URL
	 * @param initialConnectionSize The initial connection size
	 * @param maxConnectionSize The maxim connection size
	 * @param autocommit Disable auto commit if you want batch update
	 */
	public RedshiftDataSource(
			String redshiftDriverClassName, 
			String redshiftConnectionUrl, 
			Integer initialConnectionSize,
			Integer maxConnectionSize, 
			Boolean autocommit,
			String username,
			String password) {
		super();
		setRedshiftDriverClassName(redshiftDriverClassName);
		setRedshiftConnectionUrl(redshiftConnectionUrl);
		setInitialConnectionSize(initialConnectionSize);
		setMaxConnectionSize(maxConnectionSize);
		setAutocommit(autocommit);
		this.setUsername(username);
		this.setPassword(password);
	}
	
	public String getRedshiftDriverClassName() {
		return redshiftDriverClassName;
	}

	public void setRedshiftDriverClassName(String redshiftDriverClassName) {
		Preconditions.checkArgument(!Strings.isEmpty(redshiftDriverClassName), "redshiftDriverClassName must be provided");
		this.redshiftDriverClassName = redshiftDriverClassName;
		this.setDriverClassName(redshiftDriverClassName);
	}

	public String getRedshiftConnectionUrl() {
		return redshiftConnectionUrl;
	}

	public void setRedshiftConnectionUrl(String redshiftConnectionUrl) {
		Preconditions.checkArgument(!Strings.isEmpty(redshiftConnectionUrl), "redshiftConnectionUrl must be provided");
		this.redshiftConnectionUrl = redshiftConnectionUrl;
		this.setUrl(redshiftConnectionUrl);
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
}
