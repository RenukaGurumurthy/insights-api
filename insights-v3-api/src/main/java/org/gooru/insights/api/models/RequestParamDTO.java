package org.gooru.insights.api.models;

import java.io.Serializable;

public class RequestParamDTO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9198751741773294912L;
	
	private String resourceGooruId;

	private String collectionGooruId;
	
	private String classGooruId;
	
	private String unitGooruId;
	
	private String lessonGooruId;

	private String userUid;
	
	public String getResourceGooruId() {
		return resourceGooruId;
	}

	public void setResourceGooruId(String resourceGooruId) {
		this.resourceGooruId = resourceGooruId;
	}

	public String getCollectionGooruId() {
		return collectionGooruId;
	}

	public void setCollectionGooruId(String collectionGooruId) {
		this.collectionGooruId = collectionGooruId;
	}

	public String getClassGooruId() {
		return classGooruId;
	}

	public void setClassGooruId(String classGooruId) {
		this.classGooruId = classGooruId;
	}

	public String getUnitGooruId() {
		return unitGooruId;
	}

	public void setUnitGooruId(String unitGooruId) {
		this.unitGooruId = unitGooruId;
	}

	public String getLessonGooruId() {
		return lessonGooruId;
	}

	public void setLessonGooruId(String lessonGooruId) {
		this.lessonGooruId = lessonGooruId;
	}

	public String getUserUid() {
		return userUid;
	}

	public void setUserUid(String userUid) {
		this.userUid = userUid;
	}

	public String getReportType() {
		return reportType;
	}

	public void setReportType(String reportType) {
		this.reportType = reportType;
	}

	public Long getSessionId() {
		return sessionId;
	}

	public void setSessionId(Long sessionId) {
		this.sessionId = sessionId;
	}

	private String reportType;

	private Long sessionId;

}
