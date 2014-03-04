/*******************************************************************************
 * ResponseParamsDTO.java
 * insights-read-api
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.gooru.insights.api.models;

public class ResponseParamsDTO {


	private String userUId;
	
	private String levelsCodeId;
	
	private String levelsLabel;
	
	private String levelsDepth;

	private String filterAggregate;

	private String collectionTitle;
	
	private String collectionGooruOId;
	
	private String collectionContentId;
	
	private String collectionThumbnail;
	
	private String resourceTitle;
	
	private String resourceGooruOId;
	
	private String resourceContentId;
	
	private String resourceThumbnail;
	
	private String quizTitle;
	
	private String quizGooruOId;
	
	private String quizContentId;
	
	private String quizThumbnail;
	
	private String questionTitle;
	
	private String questionGooruOId;
	
	private String questionContentId;
	
	private String questionThumbnail;
	
	private String resourceCategory;
	
	private String collectionCategory;
	
	private String networkLabel;
	
	private String networkId;
	
	private String authorLabel;
	
	private String authorUId;
	
	private String sourceLabel;
	
	private String sourceId;

	private String gradeLabel;
	
	private String gradeId;
	
	private String organizationId;
	
	private String organizationLabel;
	
	private String LicenceLabel;
	
	private String LicenceId;
	
	private Long collectionTimeSpentInMs;
	
	private Integer collectionViews;
	
	private Long collectionAverageTimeSpentInMs;
	
	private Integer collectionMeasure;
	
	private String collectionReaction;
	
	private Long collectionReactionTimeSpentInMs;
	
	private String date;
	
	private String subject;
	
	private String course;
	
	private String unit;
	
	private String topic;
	
	private String lesson;
	
	private Integer time;

	private String author;
	
	private String userName;
	
	private Integer collectionUserViews;
	
	private Long collectionUserAverageTimeSpentInMs;
	
	private Long collectionUserReactionTimeSpentInMs;
	
	private String collectionUserReaction;
	
	private Long collectionUserTimeSpentInMs;
	
	private String createdOn;
	
	private String lastModified;
	
	private Long timeSpentInMs;
	
	private Integer views;
	
	private String averageTimeSpent;
	
	private Integer measure;
	
	private String title;
	
	private String thumbnail;
	
	private String gooruOId;
	
	private String reaction;
	
	private Integer contentId;
	
	private String source;
	
	private Long resourceTimeSpentInMs;
	
	private Integer resourceViews;
	
	private Long resourceAverageTimeSpent;
	
	private Integer resourceMeasure;
	
	private String reactionThumbnail;
	
	private String resourceItemSequence;

	private String resourceDescription;
	
	private String resourceTotalScore;
	
	private String resourceTotalLike;

	private String resourceSource;
	
	private String deletedResource;
	
	private String resourceReaction;
	
	private Long resourceReactionTimeSpentInMs;
	
	public Long getCollectionTimeSpentInMs() {
		return collectionTimeSpentInMs;
	}

	public void setCollectionTimeSpentInMs(Long collectionTimeSpentInMs) {
		this.collectionTimeSpentInMs = collectionTimeSpentInMs;
	}

	public Integer getCollectionViews() {
		return collectionViews;
	}

	public void setCollectionViews(Integer collectionViews) {
		this.collectionViews = collectionViews;
	}

	public Long getCollectionAverageTimeSpentInMs() {
		return collectionAverageTimeSpentInMs;
	}

	public void setCollectionAverageTimeSpentInMs(Long collectionAverageTimeSpentInMs) {
		this.collectionAverageTimeSpentInMs = collectionAverageTimeSpentInMs;
	}

	public Integer getCollectionMeasure() {
		return collectionMeasure;
	}

	public void setCollectionMeasure(Integer collectionMeasure) {
		this.collectionMeasure = collectionMeasure;
	}

	public String getCollectionReaction() {
		return collectionReaction;
	}

	public void setCollectionReaction(String collectionReaction) {
		this.collectionReaction = collectionReaction;
	}

	public Long getCollectionReactionTimeSpentInMs() {
		return collectionReactionTimeSpentInMs;
	}

	public void setCollectionReactionTimeSpentInMs(Long collectionReactionTimeSpentInMs) {
		this.collectionReactionTimeSpentInMs = collectionReactionTimeSpentInMs;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getCourse() {
		return course;
	}

	public void setCourse(String course) {
		this.course = course;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getLesson() {
		return lesson;
	}

	public void setLesson(String lesson) {
		this.lesson = lesson;
	}

	public Integer getTime() {
		return time;
	}

	public void setTime(Integer time) {
		this.time = time;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public Integer getCollectionUserViews() {
		return collectionUserViews;
	}

	public void setCollectionUserViews(Integer collectionUserViews) {
		this.collectionUserViews = collectionUserViews;
	}

	public Long getCollectionUserAverageTimeSpentInMs() {
		return collectionUserAverageTimeSpentInMs;
	}

	public void setCollectionUserAverageTimeSpentInMs(Long collectionUserAverageTimeSpentInMs) {
		this.collectionUserAverageTimeSpentInMs = collectionUserAverageTimeSpentInMs;
	}

	public Long getCollectionUserReactionTimeSpentInMs() {
		return collectionUserReactionTimeSpentInMs;
	}

	public void setCollectionUserReactionTimeSpentInMs(Long collectionUserReactionTimeSpentInMs) {
		this.collectionUserReactionTimeSpentInMs = collectionUserReactionTimeSpentInMs;
	}

	public String getCollectionUserReaction() {
		return collectionUserReaction;
	}

	public void setCollectionUserReaction(String collectionUserReaction) {
		this.collectionUserReaction = collectionUserReaction;
	}

	public Long getCollectionUserTimeSpentInMs() {
		return collectionUserTimeSpentInMs;
	}

	public void setCollectionUserTimeSpentInMs(Long collectionUserTimeSpentInMs) {
		this.collectionUserTimeSpentInMs = collectionUserTimeSpentInMs;
	}

	public String getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(String createdOn) {
		this.createdOn = createdOn;
	}

	public String getLastModified() {
		return lastModified;
	}

	public void setLastModified(String lastModified) {
		this.lastModified = lastModified;
	}

	public Long getTimeSpentInMs() {
		return timeSpentInMs;
	}

	public void setTimeSpentInMs(Long timeSpentInMs) {
		this.timeSpentInMs = timeSpentInMs;
	}

	public Integer getViews() {
		return views;
	}

	public void setViews(Integer views) {
		this.views = views;
	}

	public String getAverageTimeSpent() {
		return averageTimeSpent;
	}

	public void setAverageTimeSpent(String averageTimeSpent) {
		this.averageTimeSpent = averageTimeSpent;
	}

	public Integer getMeasure() {
		return measure;
	}

	public void setMeasure(Integer measure) {
		this.measure = measure;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getThumbnail() {
		return thumbnail;
	}

	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}

	public String getGooruOId() {
		return gooruOId;
	}

	public void setGooruOId(String gooruOId) {
		this.gooruOId = gooruOId;
	}

	public String getReaction() {
		return reaction;
	}

	public void setReaction(String reaction) {
		this.reaction = reaction;
	}

	public Integer getContentId() {
		return contentId;
	}

	public void setContentId(Integer contentId) {
		this.contentId = contentId;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public Long getResourceTimeSpentInMs() {
		return resourceTimeSpentInMs;
	}

	public void setResourceTimeSpentInMs(Long resourceTimeSpentInMs) {
		this.resourceTimeSpentInMs = resourceTimeSpentInMs;
	}

	public Integer getResourceViews() {
		return resourceViews;
	}

	public void setResourceViews(Integer resourceViews) {
		this.resourceViews = resourceViews;
	}

	public Long getResourceAverageTimeSpent() {
		return resourceAverageTimeSpent;
	}

	public void setResourceAverageTimeSpent(Long resourceAverageTimeSpent) {
		this.resourceAverageTimeSpent = resourceAverageTimeSpent;
	}

	public Integer getResourceMeasure() {
		return resourceMeasure;
	}

	public void setResourceMeasure(Integer resourceMeasure) {
		this.resourceMeasure = resourceMeasure;
	}

	public String getReactionThumbnail() {
		return reactionThumbnail;
	}

	public void setReactionThumbnail(String reactionThumbnail) {
		this.reactionThumbnail = reactionThumbnail;
	}

	public String getResourceItemSequence() {
		return resourceItemSequence;
	}

	public void setResourceItemSequence(String resourceItemSequence) {
		this.resourceItemSequence = resourceItemSequence;
	}

	public String getResourceDescription() {
		return resourceDescription;
	}

	public void setResourceDescription(String resourceDescription) {
		this.resourceDescription = resourceDescription;
	}

	public String getResourceTotalScore() {
		return resourceTotalScore;
	}

	public void setResourceTotalScore(String resourceTotalScore) {
		this.resourceTotalScore = resourceTotalScore;
	}

	public String getResourceTotalLike() {
		return resourceTotalLike;
	}

	public void setResourceTotalLike(String resourceTotalLike) {
		this.resourceTotalLike = resourceTotalLike;
	}

	public String getResourceSource() {
		return resourceSource;
	}

	public void setResourceSource(String resourceSource) {
		this.resourceSource = resourceSource;
	}

	public String getDeletedResource() {
		return deletedResource;
	}

	public void setDeletedResource(String deletedResource) {
		this.deletedResource = deletedResource;
	}

	public String getResourceReaction() {
		return resourceReaction;
	}

	public void setResourceReaction(String resourceReaction) {
		this.resourceReaction = resourceReaction;
	}

	public Long getResourceReactionTimeSpentInMs() {
		return resourceReactionTimeSpentInMs;
	}

	public void setResourceReactionTimeSpentInMs(Long resourceReactionTimeSpentInMs) {
		this.resourceReactionTimeSpentInMs = resourceReactionTimeSpentInMs;
	}

	public String getUserUId() {
		return userUId;
	}
 
	public void setUserUId(String userUId) {
		this.userUId = userUId;
	}

	public String getLevelsCodeId() {
		return levelsCodeId;
	}

	public void setLevelsCodeId(String levelsCodeId) {
		this.levelsCodeId = levelsCodeId;
	}

	public String getLevelsLabel() {
		return levelsLabel;
	}

	public void setLevelsLabel(String levelsLabel) {
		this.levelsLabel = levelsLabel;
	}

	public String getLevelsDepth() {
		return levelsDepth;
	}

	public void setLevelsDepth(String levelsDepth) {
		this.levelsDepth = levelsDepth;
	}

	public String getFilterAggregate() {
		return filterAggregate;
	}

	public void setFilterAggregate(String filterAggregate) {
		this.filterAggregate = filterAggregate;
	}

	public String getCollectionTitle() {
		return collectionTitle;
	}

	public void setCollectionTitle(String collectionTitle) {
		this.collectionTitle = collectionTitle;
	}

	public String getCollectionGooruOId() {
		return collectionGooruOId;
	}

	public void setCollectionGooruOId(String collectionGooruOId) {
		this.collectionGooruOId = collectionGooruOId;
	}

	public String getCollectionContentId() {
		return collectionContentId;
	}

	public void setCollectionContentId(String collectionContentId) {
		this.collectionContentId = collectionContentId;
	}

	public String getCollectionThumbnail() {
		return collectionThumbnail;
	}

	public void setCollectionThumbnail(String collectionThumbnail) {
		this.collectionThumbnail = collectionThumbnail;
	}

	public String getResourceTitle() {
		return resourceTitle;
	}

	public void setResourceTitle(String resourceTitle) {
		this.resourceTitle = resourceTitle;
	}

	public String getResourceGooruOId() {
		return resourceGooruOId;
	}

	public void setResourceGooruOId(String resourceGooruOId) {
		this.resourceGooruOId = resourceGooruOId;
	}

	public String getResourceContentId() {
		return resourceContentId;
	}

	public void setResourceContentId(String resourceContentId) {
		this.resourceContentId = resourceContentId;
	}

	public String getResourceThumbnail() {
		return resourceThumbnail;
	}

	public void setResourceThumbnail(String resourceThumbnail) {
		this.resourceThumbnail = resourceThumbnail;
	}

	public String getQuizTitle() {
		return quizTitle;
	}

	public void setQuizTitle(String quizTitle) {
		this.quizTitle = quizTitle;
	}

	public String getQuizGooruOId() {
		return quizGooruOId;
	}

	public void setQuizGooruOId(String quizGooruOId) {
		this.quizGooruOId = quizGooruOId;
	}

	public String getQuizContentId() {
		return quizContentId;
	}

	public void setQuizContentId(String quizContentId) {
		this.quizContentId = quizContentId;
	}

	public String getQuizThumbnail() {
		return quizThumbnail;
	}

	public void setQuizThumbnail(String quizThumbnail) {
		this.quizThumbnail = quizThumbnail;
	}

	public String getQuestionTitle() {
		return questionTitle;
	}

	public void setQuestionTitle(String questionTitle) {
		this.questionTitle = questionTitle;
	}

	public String getQuestionGooruOId() {
		return questionGooruOId;
	}

	public void setQuestionGooruOId(String questionGooruOId) {
		this.questionGooruOId = questionGooruOId;
	}

	public String getQuestionContentId() {
		return questionContentId;
	}

	public void setQuestionContentId(String questionContentId) {
		this.questionContentId = questionContentId;
	}

	public String getQuestionThumbnail() {
		return questionThumbnail;
	}

	public void setQuestionThumbnail(String questionThumbnail) {
		this.questionThumbnail = questionThumbnail;
	}

	public String getResourceCategory() {
		return resourceCategory;
	}

	public void setResourceCategory(String resourceCategory) {
		this.resourceCategory = resourceCategory;
	}

	public String getCollectionCategory() {
		return collectionCategory;
	}

	public void setCollectionCategory(String collectionCategory) {
		this.collectionCategory = collectionCategory;
	}

	public String getNetworkLabel() {
		return networkLabel;
	}

	public void setNetworkLabel(String networkLabel) {
		this.networkLabel = networkLabel;
	}

	public String getNetworkId() {
		return networkId;
	}

	public void setNetworkId(String networkId) {
		this.networkId = networkId;
	}

	public String getAuthorLabel() {
		return authorLabel;
	}

	public void setAuthorLabel(String authorLabel) {
		this.authorLabel = authorLabel;
	}

	public String getAuthorUId() {
		return authorUId;
	}

	public void setAuthorUId(String authorUId) {
		this.authorUId = authorUId;
	}

	public String getSourceLabel() {
		return sourceLabel;
	}

	public void setSourceLabel(String sourceLabel) {
		this.sourceLabel = sourceLabel;
	}

	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	public String getGradeLabel() {
		return gradeLabel;
	}

	public void setGradeLabel(String gradeLabel) {
		this.gradeLabel = gradeLabel;
	}

	public String getGradeId() {
		return gradeId;
	}

	public void setGradeId(String gradeId) {
		this.gradeId = gradeId;
	}

	public String getOrganizationId() {
		return organizationId;
	}

	public void setOrganizationId(String organizationId) {
		this.organizationId = organizationId;
	}

	public String getOrganizationLabel() {
		return organizationLabel;
	}

	public void setOrganizationLabel(String organizationLabel) {
		this.organizationLabel = organizationLabel;
	}

	public String getLicenceLabel() {
		return LicenceLabel;
	}

	public void setLicenceLabel(String licenceLabel) {
		LicenceLabel = licenceLabel;
	}

	public String getLicenceId() {
		return LicenceId;
	}

	public void setLicenceId(String licenceId) {
		LicenceId = licenceId;
	}
	

}
