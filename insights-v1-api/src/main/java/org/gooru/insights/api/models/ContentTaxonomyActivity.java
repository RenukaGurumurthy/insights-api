package org.gooru.insights.api.models;

public class ContentTaxonomyActivity {

	private String subjectId;
	
	private String courseId;
	
	private String domainId;
	
	private String subDomainId;
	
	private String standardsId;
	
	private String learningTargetsId;

	private String gooruOid;
	
	private String resourceFormat;
	
	private String resourceType;
	
	private Long views;
	
	private Long timespent;
	
	private Long score;
	
	private String userUid;
	
	public ContentTaxonomyActivity() {
	}
	
	public ContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		this.setSubjectId(contentTaxonomyActivity.getSubjectId());
		this.setCourseId(contentTaxonomyActivity.getCourseId());
		this.setDomainId(contentTaxonomyActivity.getDomainId());
		this.setSubDomainId(contentTaxonomyActivity.getSubDomainId());
		this.setStandardsId(contentTaxonomyActivity.getStandardsId());
		this.setLearningTargetsId(contentTaxonomyActivity.getLearningTargetsId());
		this.setResourceFormat(contentTaxonomyActivity.getResourceFormat());
		this.setResourceType(contentTaxonomyActivity.getResourceType());
		this.setUserUid(contentTaxonomyActivity.getUserUid());
	}
	
	ContentTaxonomyActivity(String subjectId, String courseId, String domainId, String subDomainId, String standardId, String learningTargetId, String resourceFormat, String resourceType, String userUid, Long views, Long timespent, Long score) {
		this.setSubjectId(subjectId);
		this.setCourseId(courseId);
		this.setDomainId(domainId);
		this.setSubDomainId(subDomainId);
		this.setStandardsId(standardId);
		this.setLearningTargetsId(learningTargetId);
		this.setResourceFormat(resourceFormat);
		this.setResourceType(resourceType);
		this.setUserUid(userUid);
		this.setViews(views);
		this.setTimespent(timespent);
		this.setScore(score);
	}
 
	public String getSubjectId() {
		return subjectId;
	}

	public void setSubjectId(String subjectId) {
		this.subjectId = subjectId;
	}

	public String getCourseId() {
		return courseId;
	}

	public void setCourseId(String courseId) {
		this.courseId = courseId;
	}

	public String getDomainId() {
		return domainId;
	}

	public void setDomainId(String domainId) {
		this.domainId = domainId;
	}

	public String getSubDomainId() {
		return subDomainId;
	}

	public void setSubDomainId(String subDomainId) {
		this.subDomainId = subDomainId;
	}

	public String getStandardsId() {
		return standardsId;
	}

	public void setStandardsId(String standardsId) {
		this.standardsId = standardsId;
	}

	public String getLearningTargetsId() {
		return learningTargetsId;
	}

	public void setLearningTargetsId(String learningTargetsId) {
		this.learningTargetsId = learningTargetsId;
	}

	public String getGooruOid() {
		return gooruOid;
	}

	public void setGooruOid(String gooruOid) {
		this.gooruOid = gooruOid;
	}

	public String getResourceFormat() {
		return resourceFormat;
	}

	public void setResourceFormat(String resourceFormat) {
		this.resourceFormat = resourceFormat;
	}

	public String getResourceType() {
		return resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	public Long getViews() {
		return views;
	}

	public void setViews(Long views) {
		this.views = views;
	}

	public Long getTimespent() {
		return timespent;
	}

	public void setTimespent(Long timespent) {
		this.timespent = timespent;
	}

	public Long getScore() {
		return score;
	}

	public void setScore(Long score) {
		this.score = score;
	}

	public String getUserUid() {
		return userUid;
	}

	public void setUserUid(String userUid) {
		this.userUid = userUid;
	}
	
	public static String taxonomyDepthField(ContentTaxonomyActivity contentTaxonomyActivity,Integer depth) {
		if(depth == 0) {
			return contentTaxonomyActivity.getSubjectId();
		} else if(depth == 1) {
			return contentTaxonomyActivity.getCourseId();
		} else if(depth == 2) {
			return contentTaxonomyActivity.getDomainId();
		} else if(depth == 3) {
			return contentTaxonomyActivity.getSubDomainId();
		} else if(depth == 4) {
			return contentTaxonomyActivity.getStandardsId();
		} else if(depth == 5) {
			return contentTaxonomyActivity.getLearningTargetsId();
		}
		return contentTaxonomyActivity.getSubjectId();
	}
}
