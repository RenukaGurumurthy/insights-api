package org.gooru.insights.api.models;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.gooru.insights.api.constants.ApiConstants;

public class SessionTaxonomyActivity implements Cloneable {

	private String sessionId;

	private String subjectId;

	private String courseId;

	private String domainId;

	private String standardsId;

	private String learningTargetsId;

	private Set<String> subjectIds;

	private Set<String> courseIds;

	private Set<String> domainIds;

	private Set<String> standardsIds;

	private Set<String> learningTargetsIds;

	private Long views;

	private Long timespent;

	private Long attempts;

	private Long score;

	private String questionType;

	private String resourceType;

	private String answerStatus;

	private String resourceId;

	private String questionId;

	private Long reaction;

	private List<SessionTaxonomyActivity> questions;

	private Long totalAttemptedQuestions;

	private String displayCode;

	public Object clone()throws CloneNotSupportedException{
		return super.clone();
	}

	public Long getTotalAttemptedQuestions() {
		return totalAttemptedQuestions;
	}

	public void setTotalAttemptedQuestions(Long totalAttemptedQuestions) {
		this.totalAttemptedQuestions = totalAttemptedQuestions;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
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

	public Long getAttempts() {
		return attempts;
	}

	public void setAttempts(Long attempts) {
		this.attempts = attempts;
	}

	public Long getScore() {
		return score;
	}

	public void setScore(Long score) {
		this.score = score;
	}

	public String getQuestionType() {
		return questionType;
	}

	public void setQuestionType(String questionType) {
		this.questionType = questionType;
	}

	public String getResourceType() {
		return resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	public String getAnswerStatus() {
		return answerStatus;
	}

	public void setAnswerStatus(String answerStatus) {
		this.answerStatus = answerStatus;
	}

	public String getResourceId() {
		return resourceId;
	}

	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}

	public String getQuestionId() {
		return questionId;
	}

	public void setQuestionId(String questionId) {
		this.questionId = questionId;
	}

	public Long getReaction() {
		return reaction;
	}

	public void setReaction(Long reaction) {
		this.reaction = reaction;
	}

	public List<SessionTaxonomyActivity> getQuestions() {
		return questions;
	}

	public void setQuestions(List<SessionTaxonomyActivity> questionsData) {
		if(this.questions == null) {
			this.questions = new ArrayList<>();
			for(SessionTaxonomyActivity questionData : questionsData) {
				questions.add(reAllocateObjects(questionData));
			}
		}
	}

	public SessionTaxonomyActivity() {
		this.totalAttemptedQuestions = 1L;
	}

	private SessionTaxonomyActivity(SessionTaxonomyActivity obj) {
		this.timespent = obj.timespent;
		this.attempts = obj.attempts;
		this.score = obj.score;
		this.questionType = obj.questionType;
		this.questionId = obj.questionId;
		this.answerStatus = obj.answerStatus;
		this.reaction = obj.reaction;
	}

	public static String getGroupByField(SessionTaxonomyActivity obj1, String levelType) {
		switch(levelType) {
		case ApiConstants.SUBJECT:
			return obj1.courseId;
		case ApiConstants.COURSE:
			return obj1.domainId;
		case ApiConstants.DOMAIN:
			return obj1.standardsId;
		case ApiConstants.STANDARDS:
			return obj1.learningTargetsId;
		}
		return obj1.domainId;
	}
	private SessionTaxonomyActivity reAllocateObjects(SessionTaxonomyActivity questionData) {
			return new SessionTaxonomyActivity(questionData);
	}

	public Set<String> getSubjectIds() {
		return subjectIds;
	}

	public void setSubjectIds(String subjectIds) {
		if(this.subjectIds  == null){
			this.subjectIds = new HashSet<>();
		}
		this.subjectIds.add(subjectIds);
	}

	public Set<String> getCourseIds() {
		return courseIds;
	}

	public void setCourseIds(String courseIds) {
		if(this.courseIds  == null){
			this.courseIds = new HashSet<>();
		}
		this.courseIds.add(courseIds);
	}

	public Set<String> getDomainIds() {
		return domainIds;
	}

	public void setDomainIds(String domainIds) {
		if(this.domainIds  == null){
			this.domainIds = new HashSet<>();
		}
		this.domainIds.add(domainIds);
	}

	public Set<String> getStandardsIds() {
		return standardsIds;
	}

	public void setStandardsIds(String standardsIds) {
		if(this.standardsIds  == null){
			this.standardsIds = new HashSet<>();
		}
		this.standardsIds.add(standardsIds);
	}

	public String getDisplayCode() {
		return displayCode;
	}

	public void setDisplayCode(String disPlayCode) {
		this.displayCode = disPlayCode;
	}

	public Set<String> getLearningTargetsIds() {
		return learningTargetsIds;
	}

	public void setLearningTargetsIds(String learningTargetsIds) {
		if(this.learningTargetsIds  == null){
			this.learningTargetsIds = new HashSet<>();
		}
		this.learningTargetsIds.add(learningTargetsIds);
	}
}
