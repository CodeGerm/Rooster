package org.cg.rooster.phoenix;

import java.util.Date;

import org.cg.rooster.JdbcDataRepository;
import org.springframework.data.domain.Persistable;

/**
 * A example event POJO
 * @author WZ
 *
 */
public class Event implements Persistable<Object[]> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3667252197918887146L;
	
	private int tenantId;
	private String userId;
	private Date eventTime;
	private Date receiptTime;
	
	private String name;
	private String message;
	private int version;
	
	public Event() {
		super();
	}

	public int getTenantId() {
		return tenantId;
	}
	
	public void setTenantId(int tenantId) {
		this.tenantId = tenantId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Date getEventTime() {
		return eventTime;
	}

	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}

	public Date getReceiptTime() {
		return receiptTime;
	}

	public void setReceiptTime(Date receiptTime) {
		this.receiptTime = receiptTime;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public Object[] getId() {
		return JdbcDataRepository.primaryKey(tenantId, userId, eventTime, receiptTime);
	}

	@Override
	public boolean isNew() {
		//not needed for Phoenix
		return false;
	}

}
