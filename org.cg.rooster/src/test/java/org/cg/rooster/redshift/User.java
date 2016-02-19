package org.cg.rooster.redshift;

import java.util.Date;
import org.cg.rooster.JdbcDataRepository;
import org.springframework.data.domain.Persistable;

/**
 * A example user POJO
 * @author WZ
 *
 */
public class User implements Persistable<Object[]> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5033352789548106538L;
	
	private String id;
	private String userName;
	private String displayName;
	private String email;
	private Date lastLogin;
	private Date lastInvite;
	private String status;
	private String statusEnum;
	private String sourceDs;
	private String sourceDsType;
	private String forest;
	private String directoryServiceUuidl;
	private String sourceDsLocalized;
	
	
	public User() {
		super();
	}

	/**
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * @param userName the userName to set
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}

	/**
	 * @return the displayName
	 */
	public String getDisplayName() {
		return displayName;
	}

	/**
	 * @param displayName the displayName to set
	 */
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	/**
	 * @return the email
	 */
	public String getEmail() {
		return email;
	}

	/**
	 * @param email the email to set
	 */
	public void setEmail(String email) {
		this.email = email;
	}

	/**
	 * @return the lastLogin
	 */
	public Date getLastLogin() {
		return lastLogin;
	}

	/**
	 * @param lastLogin the lastLogin to set
	 */
	public void setLastLogin(Date lastLogin) {
		this.lastLogin = lastLogin;
	}

	/**
	 * @return the lastInvite
	 */
	public Date getLastInvite() {
		return lastInvite;
	}

	/**
	 * @param lastInvite the lastInvite to set
	 */
	public void setLastInvite(Date lastInvite) {
		this.lastInvite = lastInvite;
	}

	/**
	 * @return the status
	 */
	public String getStatus() {
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(String status) {
		this.status = status;
	}

	/**
	 * @return the statusEnum
	 */
	public String getStatusEnum() {
		return statusEnum;
	}

	/**
	 * @param statusEnum the statusEnum to set
	 */
	public void setStatusEnum(String statusEnum) {
		this.statusEnum = statusEnum;
	}

	/**
	 * @return the sourceDs
	 */
	public String getSourceDs() {
		return sourceDs;
	}

	/**
	 * @param sourceDs the sourceDs to set
	 */
	public void setSourceDs(String sourceDs) {
		this.sourceDs = sourceDs;
	}

	/**
	 * @return the sourceDsType
	 */
	public String getSourceDsType() {
		return sourceDsType;
	}

	/**
	 * @param sourceDsType the sourceDsType to set
	 */
	public void setSourceDsType(String sourceDsType) {
		this.sourceDsType = sourceDsType;
	}

	/**
	 * @return the forest
	 */
	public String getForest() {
		return forest;
	}

	/**
	 * @param forest the forest to set
	 */
	public void setForest(String forest) {
		this.forest = forest;
	}

	/**
	 * @return the directoryServiceUuidl
	 */
	public String getDirectoryServiceUuidl() {
		return directoryServiceUuidl;
	}

	/**
	 * @param directoryServiceUuidl the directoryServiceUuidl to set
	 */
	public void setDirectoryServiceUuidl(String directoryServiceUuidl) {
		this.directoryServiceUuidl = directoryServiceUuidl;
	}

	/**
	 * @return the sourceDsLocalized
	 */
	public String getSourceDsLocalized() {
		return sourceDsLocalized;
	}

	/**
	 * @param sourceDsLocalized the sourceDsLocalized to set
	 */
	public void setSourceDsLocalized(String sourceDsLocalized) {
		this.sourceDsLocalized = sourceDsLocalized;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "User [id=" + id + ", userName=" + userName + ", displayName="
				+ displayName + ", email=" + email + ", lastLogin=" + lastLogin
				+ ", lastInvite=" + lastInvite + ", status=" + status
				+ ", statusEnum=" + statusEnum + ", sourceDs=" + sourceDs
				+ ", sourceDsType=" + sourceDsType + ", forest=" + forest
				+ ", directoryServiceUuidl=" + directoryServiceUuidl
				+ ", sourceDsLocalized=" + sourceDsLocalized + "]";
	}

	@Override
	public Object[] getId() {
		return JdbcDataRepository.primaryKey(id);
	}

	@Override
	public boolean isNew() {
		//not needed for redshift
		return false;
	}

}
