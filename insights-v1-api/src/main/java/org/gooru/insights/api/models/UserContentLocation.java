package org.gooru.insights.api.models;

import java.io.Serializable;

public class UserContentLocation implements Serializable {

	private static final long serialVersionUID = -5100648457462725007L;

	public UserContentLocation(String classUid, String courseUid, String unitUid, String lessonUid, String collectionUid, String userUid, String collectionType) {
		super();
		this.classUid = classUid;
		this.courseUid = courseUid;
		this.unitUid = unitUid;
		this.lessonUid = lessonUid;
		this.collectionUid = collectionUid;
		this.userUid = userUid;
		this.collectionType = collectionType;
	}

	private String classUid;

	private String courseUid;

	private String unitUid;

	private String lessonUid;

	private String collectionUid;

	private String userUid;

	private String collectionType;

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

	@Override
	public String toString() {
		return String.format("UserContentLocation(%s,%s,%s,%s,%s,%s,%s)", classUid, courseUid, unitUid, lessonUid, collectionUid, userUid, collectionType);
	}

}
