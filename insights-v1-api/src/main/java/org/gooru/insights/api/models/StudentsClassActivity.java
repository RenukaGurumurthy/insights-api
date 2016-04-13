package org.gooru.insights.api.models;

import java.io.Serializable;

public class StudentsClassActivity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8407967292421744242L;

	private String classUid;

	private String courseUid;
	
	private String unitUid;

	private String lessonUid;
	
	private String collectionUid;
	
	private String userUid;

	private String collectionType;
	
	private String attemptStatus;

	private long views;

	private long timeSpent;

	private long score;

	private long reaction;

	public String getClassUid() {
		return classUid;
	}

	public void setClassUid(String classUid) {
		this.classUid = classUid;
	}

	public String getCourseUid() {
		return courseUid;
	}

	public void setCourseUid(String courseUid) {
		this.courseUid = courseUid;
	}

	public String getUnitUid() {
		return unitUid;
	}

	public void setUnitUid(String unitUid) {
		this.unitUid = unitUid;
	}

	public String getLessonUid() {
		return lessonUid;
	}

	public void setLessonUid(String lessonUid) {
		this.lessonUid = lessonUid;
	}

	public String getCollectionUid() {
		return collectionUid;
	}

	public void setCollectionUid(String collectionUid) {
		this.collectionUid = collectionUid;
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

	public void setViews(long views) {
		this.views = views;
	}

	public long getTimeSpent() {
		return timeSpent;
	}

	public void setTimeSpent(long timeSpent) {
		this.timeSpent = timeSpent;
	}

	public long getScore() {
		return score;
	}

	public void setScore(long score) {
		this.score = score;
	}

	public long getReaction() {
		return reaction;
	}

	public void setReaction(long reaction) {
		this.reaction = reaction;
	}

	public static String aggregateDepth(StudentsClassActivity sca, String level) {
		switch (level) {
		case "unit":
			return sca.unitUid;
		case "lesson":
			return sca.lessonUid;
		case "collection":
			return sca.collectionUid;
		default:
			break;
		}
		return sca.courseUid;
	}
}
