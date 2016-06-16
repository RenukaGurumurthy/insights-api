package org.gooru.insights.api.models;

import java.io.Serializable;

import org.gooru.insights.api.constants.ApiConstants;

public class StudentsClassActivity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8407967292421744242L;

	private String classId;

	private String courseId;
	
	private String unitId;

	private String lessonId;
	
	private String collectionId;
	
	private String userUid;

	private String collectionType;
	
	private String attemptStatus;

	private Long views;

	private Long timeSpent;

	private Long score;

	private Long reaction;

	private Long scoreInPercentage;
	
	private Long totalCount;
	
	private Long completedCount;
	
	private Long attempts;
	
	private String assessmentId;
	
	public String getClassId() {
		return classId;
	}

	public void setClassId(String classId) {
		this.classId = classId;
	}

	public String getCourseId() {
		return courseId;
	}

	public void setCourseId(String courseId) {
		this.courseId = courseId;
	}

	public String getUnitId() {
		return unitId;
	}

	public void setUnitId(String unitId) {
		this.unitId = unitId;
	}

	public String getLessonId() {
		return lessonId;
	}

	public void setLessonId(String lessonId) {
		this.lessonId = lessonId;
	}

	public String getCollectionId() {
		return collectionId;
	}

	public void setCollectionId(String collectionId) {
		this.collectionId = collectionId;
	}

	public String getUserUid() {
		return userUid;
	}

	public void setUserUid(String userUid) {
		this.userUid = userUid;
	}

	public String getCollectionType() {
		return collectionType;
	}

	public void setCollectionType(String collectionType) {
		this.collectionType = collectionType;
	}

	public String getAttemptStatus() {
		return attemptStatus;
	}

	public void setAttemptStatus(String attemptStatus) {
		this.attemptStatus = attemptStatus;
	}

	public long getViews() {
		return views;
	}

	public void setViews(Long views) {
		this.views = views;
	}

	public long getTimeSpent() {
		return timeSpent;
	}

	public void setTimeSpent(Long timeSpent) {
		this.timeSpent = timeSpent;
	}

	public long getScore() {
		return score;
	}

	public void setScore(Long score) {
		this.score = score;
	}

	public long getReaction() {
		return reaction;
	}

	public void setReaction(Long reaction) {
		this.reaction = reaction;
	}

	public static String aggregateDepth(StudentsClassActivity sca, String level) {
		switch (level) {
		case ApiConstants.UNIT:
			return sca.unitId;
		case ApiConstants.LESSON:
			return sca.lessonId;
		case ApiConstants.CONTENT:
			if (ApiConstants.COLLECTION.equals(sca.collectionType)) {
				return sca.collectionId;				
			}else if (ApiConstants.ASSESSMENT.equals(sca.collectionType)){
			  return sca.assessmentId;
			} else{
			  return sca.collectionId;
			}
		default:
			break;
		}
		return sca.courseId;
	}

	public Long getScoreInPercentage() {
		return scoreInPercentage;
	}

	public void setScoreInPercentage(Long score, Long totalCount) {
		this.scoreInPercentage = (totalCount == null || totalCount == 0 || score == null) ? 0 : Math.round((double)(score /totalCount));
	}

	public Long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(Long totalCount) {
		this.totalCount = totalCount;
	}

	public Long getCompletedCount() {
		return completedCount;
	}

	public void setCompletedCount(Long completedCount) {
		this.completedCount = (this.completedCount == null) ? completedCount : this.completedCount + completedCount;
	}

	public Long getAttempts() {
		return attempts;
	}

	public void setAttempts(Long attempts) {
		this.attempts = attempts;
	}

	public String getAssessmentId() {
		return assessmentId;
	}

	public void setAssessmentId(String assessmentId) {
		this.assessmentId = assessmentId;
	}
}
