package org.cg.rooster.impala;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.hbase.util.Strings;

import com.google.common.base.Preconditions;

public class ImpalaDataSource extends BasicDataSource {
	
	protected String impalaDriverClassName; 
	protected String impalaConnectionUrl;
	protected Integer initialConnectionSize;
	protected Integer maxConnectionSize;
	protected Boolean autocommit;

	public ImpalaDataSource() {

	}
	
	/**
	 * 
	 * @param impalaDriverClassName The JDBC driver class name
	 * @param impalaConnectionUrl The connection URL
	 */
	public ImpalaDataSource(
			String impalaDriverClassName, 
			String impalaConnectionUrl,
			String username,
			String password) {
		super();
		setImpalaDriverClassName(impalaDriverClassName);
		setImpalaConnectionUrl(impalaConnectionUrl);
		this.setUsername(username);
		this.setPassword(password);
	}
	
	/**
	 * 
	 * @param impalaDriverClassName The JDBC driver class name
	 * @param impalaConnectionUrl The connection URL
	 * @param initialConnectionSize The initial connection size
	 * @param maxConnectionSize The maxim connection size
	 * @param autocommit Disable auto commit if you want batch update
	 */
	public ImpalaDataSource(
			String impalaDriverClassName, 
			String impalaConnectionUrl, 
			Integer initialConnectionSize,
			Integer maxConnectionSize, 
			Boolean autocommit,
			String username,
			String password) {
		super();
		setImpalaDriverClassName(impalaDriverClassName);
		setImpalaConnectionUrl(impalaConnectionUrl);
		setInitialConnectionSize(initialConnectionSize);
		setMaxConnectionSize(maxConnectionSize);
		setAutocommit(autocommit);
		this.setUsername(username);
		this.setPassword(password);
	}
	
	public String getImpalaDriverClassName() {
		return impalaDriverClassName;
	}

	public void setImpalaDriverClassName(String impalaDriverClassName) {
		Preconditions.checkArgument(!Strings.isEmpty(impalaDriverClassName), "impalaDriverClassName must be provided");
		this.impalaDriverClassName = impalaDriverClassName;
		this.setDriverClassName(impalaDriverClassName);
	}

	public String getImpalaConnectionUrl() {
		return impalaConnectionUrl;
	}

	public void setImpalaConnectionUrl(String impalaConnectionUrl) {
		Preconditions.checkArgument(!Strings.isEmpty(impalaConnectionUrl), "impalaConnectionUrl must be provided");
		this.impalaConnectionUrl = impalaConnectionUrl;
		this.setUrl(impalaConnectionUrl);
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
