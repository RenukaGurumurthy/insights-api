package org.gooru.insights.api.models;

public class ContentTaxonomyActivity {

	private String subjectId;

	private String courseId;

	private String domainId;

	private String standardsId;

	private String learningTargetsId;

	private String gooruOid;

	private String resourceFormat;

	private String resourceType;

	private Long views;

	private Long attempts;

	private Long timespent;

	private Long score;

	private String userUid;

	private Long attemptedItemCount;

	private Long scoreInPercentage;

	private Long itemCount;

	private String displayCode;

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

	public Long getAttempts() {
		return attempts;
	}

	public void setAttempts(Long attempts) {
		this.attempts = attempts;
	}

	public Long getAttemptedItemCount() {
		return attemptedItemCount;
	}

	public void setAttemptedItemCount(Long itemCount) {
		this.attemptedItemCount = itemCount;
	}

	public ContentTaxonomyActivity() {
	}

	public ContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity, Integer depth) {

		switch(depth) {
		case 1:
			this.setCourseId(contentTaxonomyActivity.courseId);
			break;
		case 2:
			this.setDomainId(contentTaxonomyActivity.domainId);
			break;
		case 3:
			this.setStandardsId(contentTaxonomyActivity.standardsId);
			break;
		case 4:
			this.setLearningTargetsId(contentTaxonomyActivity.learningTargetsId);
			break;
		}
	}

	public static String taxonomyDepthField(ContentTaxonomyActivity contentTaxonomyActivity,Integer depth) {

		switch(depth) {
		case 1:
			return contentTaxonomyActivity.courseId;
		case 2:
			return contentTaxonomyActivity.domainId;
		case 3:
			return contentTaxonomyActivity.standardsId;
		case 4:
			return contentTaxonomyActivity.learningTargetsId;
		}
		return contentTaxonomyActivity.courseId;
	}

	public Long getScoreInPercentage() {
		return scoreInPercentage;
	}

	public void setScoreInPercentage(Long scoreInPercentage) {
		this.scoreInPercentage = scoreInPercentage;
	}

	public Long getItemCount() {
		return itemCount;
	}

	public void setItemCount(Long itemCount) {
		this.itemCount = itemCount;
	}

	public String getDisplayCode() {
		return displayCode;
	}

	public void setDisplayCode(String displayCode) {
		this.displayCode = displayCode;
	}
}
