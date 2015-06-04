package org.gooru.insights.api.models;


public class RequestParamsFiltersDTO {

	private static final long serialVersionUID = -284059979698775789L;

	private String userUId;
	
	private String sessionId;
	
	private String classId;
	
	private String session;
	
	private String collectionGooruOId;
	
	private String pathwayId;
	
	private String resourceGooruOId;
	
	private String authorUId;
	
	private String organizationId;
	
	private String endDate;
	
	private String apiKey;
	
	private String eventName;
	
	private String eventParam;
	
	public String getClassId() {
		return classId;
	}

	public void setClassId(String classId) {
		this.classId = classId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getUserUId() {
		return userUId;
	}

	public void setUserUId(String userUId) {
		this.userUId = userUId;
	}

	public String getCollectionGooruOId() {
		return collectionGooruOId;
	}

	public void setCollectionGooruOId(String collectionGooruOId) {
		this.collectionGooruOId = collectionGooruOId;
	}

	public String getResourceGooruOId() {
		return resourceGooruOId;
	}

	public void setResourceGooruOId(String resourceGooruOId) {
		this.resourceGooruOId = resourceGooruOId;
	}

	public String getAuthorUId() {
		return authorUId;
	}

	public void setAuthorUId(String authorUId) {
		this.authorUId = authorUId;
	}

	public String getOrganizationId() {
		return organizationId;
	}

	public void setOrganizationId(String organizationId) {
		this.organizationId = organizationId;
	}

	public String getPathwayId() {
		return pathwayId;
	}

	public void setPathwayId(String pathwayId) {
		this.pathwayId = pathwayId;
	}

	public String getStartDate() {
		return null;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getApiKey() {
		return apiKey;
	}

	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	public String getEventParam() {
		return eventParam;
	}

	public void setEventParam(String eventParam) {
		this.eventParam = eventParam;
	}

}
