package org.gooru.insights.api.services;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.spring.exception.BadRequestException;
import org.springframework.stereotype.Service;

@Service
public class SelectParamsServiceImpl implements SelectParamsService {

	private static Map<String,Map<String,String>> selectParam;
	
	@PostConstruct
	private void init() {
		putSelectValues();
	}

	private void putSelectValues() {
		selectParam = new HashMap<String, Map<String, String>>();
		putLiveDashboardCache();
		putClasspageCollectionOEResources();
		putClasspageDetail();
		putClassExportDetail();
		putAggCollectionExportDetail();
		putClassResourceExportDetail();
		putClasspageCollectionDetail();
		putClasspageCollectionUsage();
		putClasspageResourceUsage();
		putUser();
		putAssessmentAnswer();
		putClasspageUser();
		putOEResource();
		putUserPreference();
		putUserPreferenceData();
	}

	private void putLiveDashboardCache(){
		
		Map<String, String> liveDashboard = new HashMap<String,String>();
		liveDashboard.put("timeSpent", "total");
		liveDashboard.put("views", "views");
		liveDashboard.put("reaction", "reactions");
		liveDashboard.put("rate", "rate");
		liveDashboard.put("description", "description");
		liveDashboard.put("gooruOId", "gooru_oid");
		liveDashboard.put("title", "title");
		liveDashboard.put("resourceFormat", "resourceFormat");
		liveDashboard.put("thumbnail", "url");
		liveDashboard.put("sharing", "sharing");
		liveDashboard.put("typeName", "type_name");
		liveDashboard.put("createrUId", "user_uid");
		liveDashboard.put("licenseName", "license_name");
		liveDashboard.put("createdOn", "created_on");
		liveDashboard.put("key", "key");
		selectParam.put("liveDashboard", liveDashboard);
	}

	private void putClasspageCollectionOEResources(){
		
		Map<String, String> classPageCollectionOEResource = new HashMap<String,String>();
		classPageCollectionOEResource.put("text", "~choice");
		classPageCollectionOEResource.put("questionType", "question_type");
		classPageCollectionOEResource.put("score", "~score");
		classPageCollectionOEResource.put("resourceGooruOId", "gooru_oid");
		classPageCollectionOEResource.put("attemptStatus", "~question_status");
		classPageCollectionOEResource.put("userName", "username");
		classPageCollectionOEResource.put("gooruUId", "gooru_uid");
		classPageCollectionOEResource.put("timeSpent", "~time_spent");
		classPageCollectionOEResource.put("avgReaction", "~avg_reaction");
		classPageCollectionOEResource.put("views", "~views");
		classPageCollectionOEResource.put("avgTimeSpent", "~avg_time_spent");
		classPageCollectionOEResource.put("collectionGooruOId", "collection_gooru_oid");
		classPageCollectionOEResource.put("title", "title");
		classPageCollectionOEResource.put("resourceFormat", "resourceFormat");
		classPageCollectionOEResource.put("description", "description");
		classPageCollectionOEResource.put("lastModified", "last_modified");
		classPageCollectionOEResource.put("category", "category");
		classPageCollectionOEResource.put("thumbnail", "thumbnail");
		classPageCollectionOEResource.put("reaction", "~RA");
		classPageCollectionOEResource.put("skip", "~skipped");
		classPageCollectionOEResource.put("status", "deleted");
		classPageCollectionOEResource.put("userCount", "userCount");
		classPageCollectionOEResource.put("userData", "userData");
		classPageCollectionOEResource.put("itemSequence", "item_sequence");
		classPageCollectionOEResource.put("hasFrameBreaker", "hasFrameBreaker");		
		selectParam.put("classPageCollectionOEResource", classPageCollectionOEResource);
	}
	
	private void putClasspageDetail(){
		
		Map<String, String> classpageDetail = new HashMap<String,String>();
		classpageDetail.put("classpageGooruOId", "gooru_oid");
		classpageDetail.put("collectionItemSequence", "item_sequence");
		classpageDetail.put("collectionGooruOId", "resource_gooru_oid");
		classpageDetail.put("classpageTitle", "title");
		classpageDetail.put("classpageCreatedOn", "created_on");
		classpageDetail.put("classpageViews", "~views");
		classpageDetail.put("classpageTimeSpent", "~time_spent");
		classpageDetail.put("classpageAvgTimeSpent", "~avg_time_spent");
		selectParam.put("classpageDetail", classpageDetail);
	}
	
	private void putClassExportDetail(){
		
		Map<String, String> classExportDetail = new HashMap<String,String>();
		classExportDetail.put("collectionGooruOId", "gooru_oid");
		classExportDetail.put("collectionTitle", "title");
		classExportDetail.put("collectionCreatedOn", "created_on");
		classExportDetail.put("collectionItemSequence", "item_sequence");
		selectParam.put("classExportDetail", classExportDetail);
	}
	
	private void putAggCollectionExportDetail(){
		
		Map<String, String> collectionExportDetail = new HashMap<String,String>();
		collectionExportDetail.put("collectionTitle", "collectionTitle");
		collectionExportDetail.put("collectionCreatedOn", "collectionCreatedOn");
		collectionExportDetail.put("collectionItemSequence", "collectionItemSequence");
		collectionExportDetail.put("collectionGooruOId", "collectionGooruOId");
		collectionExportDetail.put("collectionViews", "~views");
		collectionExportDetail.put("collectionTimeSpent", "~time_spent");
		collectionExportDetail.put("collectionAvgTimeSpent", "~avg_time_spent");
		collectionExportDetail.put("collectionScore", "~score");
		collectionExportDetail.put("collectionScoreInPercent", "~scoreInPercentage");	
		selectParam.put("collectionExportDetail", collectionExportDetail);
	}
	
	private void putClassResourceExportDetail(){
		
		Map<String, String> classResourceExportDetail = new HashMap<String,String>();
		classResourceExportDetail.put("resourceGooruOId", "gooru_oid");
		classResourceExportDetail.put("resourceTitle", "title");
		classResourceExportDetail.put("resourceCreatedOn", "created_on");
		classResourceExportDetail.put("resourceViews", "~views");
		classResourceExportDetail.put("resourceTimeSpent", "~time_spent");
		classResourceExportDetail.put("resourceAvgTimeSpent", "~avg_time_spent");
		selectParam.put("classResourceExportDetail", classResourceExportDetail);
	}
	
	private void putClasspageCollectionDetail(){
		
		Map<String, String> classCollectionDetail = new HashMap<String,String>();
		classCollectionDetail.put("collectionItemSequence", "item_sequence");
		classCollectionDetail.put("collectionGooruOId", "gooru_oid");
		classCollectionDetail.put("collectionTitle", "title");
		classCollectionDetail.put("classpageTitle", "title");
		classCollectionDetail.put("collectionCreatedOn", "created_on");
		classCollectionDetail.put("collectionLastModified", "last_modified");
		classCollectionDetail.put("collectionTimeSpent", "~time_spent");
		classCollectionDetail.put("collectionAvgTimeSpent", "~avg_time_spent");
		classCollectionDetail.put("collectionviews", "~views");
		selectParam.put("classCollectionDetail", classCollectionDetail);
	}
	
	private void putClasspageCollectionUsage(){
		
		Map<String, String> classCollectionUsage = new HashMap<String,String>();
		classCollectionUsage.put("timeSpent", "~time_spent");
		classCollectionUsage.put("views", "~views");
		classCollectionUsage.put("avgTimeSpent", "~avg_time_spent");
		classCollectionUsage.put("text", "~choice");
		classCollectionUsage.put("gooruOId", "gooruOId");
		classCollectionUsage.put("avgReaction", "~avg_reaction");
		classCollectionUsage.put("title", "title");
		classCollectionUsage.put("lastAccessed", "lastAccessed");
		classCollectionUsage.put("description", "goals");
		classCollectionUsage.put("lastModified", "last_modified");
		classCollectionUsage.put("category", "category");
		classCollectionUsage.put("thumbnail", "thumbnail");
		classCollectionUsage.put("score", "~score");
		classCollectionUsage.put("options", "~A,~A~status,~B,~B~status,~C,~C~status,~D~status,~D,~E,~E~status,~options,dataSet");
		classCollectionUsage.put("selectedOptions", "~options");
		classCollectionUsage.put("totalAttemptUserCount", "~tau");
		classCollectionUsage.put("totalCorrectCount", "~correct");
		classCollectionUsage.put("userCount", "userCount");
		classCollectionUsage.put("totalInCorrectCount", "~in-correct");
		classCollectionUsage.put("skip", "~skipped");
		classCollectionUsage.put("completionStatus", "~completion_progress");
		classCollectionUsage.put("gradeInPercentage", "~grade_in_percentage");
		classCollectionUsage.put("totalQuestionCount", "totalQuestionCount");
		classCollectionUsage.put("nonResourceCount", "nonResourceCount");
		classCollectionUsage.put("resourceCount", "resourceCount");
		classCollectionUsage.put("itemCount", "itemCount");
		classCollectionUsage.put("hasFrameBreaker", "statistics.hasFrameBreakerN");
		selectParam.put("classCollectionUsage", classCollectionUsage);
	}
	
	private void putUser(){
		
		Map<String, String> userDetail = new HashMap<String,String>();
		userDetail.put("accountUId", "account_uid");
		userDetail.put("emailId", "external_id");
		userDetail.put("firstName", "firstname");
		userDetail.put("gooruUId", "gooru_uid");
		userDetail.put("lastLogin", "last_login");
		userDetail.put("lastName", "lastname");
		userDetail.put("organizationUId", "organization_uid");
		userDetail.put("registeredOn", "registered_on");
		userDetail.put("userName", "username");
		selectParam.put("userDetail", userDetail);
	}
	
	private void putClasspageResourceUsage(){
		
		Map<String, String> classpageResourceUsage = new HashMap<String,String>();
		classpageResourceUsage.put("timeSpent", "~time_spent");
		classpageResourceUsage.put("views", "~views");
		classpageResourceUsage.put("avgTimeSpent", "~avg_time_spent");
		classpageResourceUsage.put("text", "~choice");
		classpageResourceUsage.put("questionType", "~type");
		classpageResourceUsage.put("type", "question_type");
		classpageResourceUsage.put("score", "~score");
		classpageResourceUsage.put("attemptStatus", "~question_status");
		classpageResourceUsage.put("resourceGooruOId", "gooruOId");
		classpageResourceUsage.put("userName", "username");
		classpageResourceUsage.put("gooruUId", "gooruUid");
		classpageResourceUsage.put("avgReaction", "~avg_reaction");
		classpageResourceUsage.put("reaction", "~RA");
		classpageResourceUsage.put("collectionStatus", "collection_status");
		classpageResourceUsage.put("collectionGooruOId", "collection_gooru_oid");
		classpageResourceUsage.put("title", "title");
		classpageResourceUsage.put("description", "description");
		classpageResourceUsage.put("lastModified", "lastModified");
		classpageResourceUsage.put("category", "category");
		classpageResourceUsage.put("thumbnail", "thumbnail");
		classpageResourceUsage.put("options", "~A,~A~status,~B,~B~status,~C,~C~status,~D~status,~D,~E,~E~status,~options,dataSet");
		classpageResourceUsage.put("metaData", "metaData");
		classpageResourceUsage.put("userData", "userData");
		classpageResourceUsage.put("skip", "~skipped");
		classpageResourceUsage.put("totalAttemptUserCount", "~tau");
		classpageResourceUsage.put("attempts", "~attempts");
		classpageResourceUsage.put("totalCorrectCount", "~correct");
		classpageResourceUsage.put("totalInCorrectCount", "~in-correct");
		classpageResourceUsage.put("status", "deleted");
		classpageResourceUsage.put("userCount", "userCount");
		classpageResourceUsage.put("itemSequence", "item_sequence");
		classpageResourceUsage.put("gradeInPercentage", "~grade_in_percentage");
		classpageResourceUsage.put("totalQuestionCount", "~question_count");
		classpageResourceUsage.put("answerObject", "~answer_object");
		classpageResourceUsage.put("feedbackStatus", "~active");
		classpageResourceUsage.put("feedbackText", "~feed_back");
		classpageResourceUsage.put("feedbackProviderUId", "~feed_back_provider");
		classpageResourceUsage.put("feedbackTimestamp", "~feed_back_timestamp");
		classpageResourceUsage.put("feedbackTeacherName", "teachername");
		classpageResourceUsage.put("hasFrameBreaker", "statistics.hasFrameBreakerN");
		classpageResourceUsage.put("organization_uid", "organization.partyUid");
		// for Goals
		classpageResourceUsage.put("isRequired", "is_required");
		classpageResourceUsage.put("minimumScore", "minimum_score");
		classpageResourceUsage.put("estimatedTime", "estimated_time");

		// user data
		classpageResourceUsage.put("lastName", "lastname");
		classpageResourceUsage.put("createdOn", "createdOn");
		classpageResourceUsage.put("firstName", "firstname");
		classpageResourceUsage.put("emailId", "external_id");
		classpageResourceUsage.put("resourceFormat", "resourceFormat");
		classpageResourceUsage.put("associatedDate", "association_date");
		classpageResourceUsage.put("profileUrl", "profile_url");
		selectParam.put("classpageResourceUsage", classpageResourceUsage);

	}
	
	private void putAssessmentAnswer(){
		
		Map<String, String> assessmentAnswer = new HashMap<String,String>();
		assessmentAnswer.put("answerId","answer_id");
		assessmentAnswer.put("collectionGooruOId", "collection_gooru_oid");
		assessmentAnswer.put("correct", "is_correct");
		assessmentAnswer.put("questionGooruOId", "question_gooru_oid");
		assessmentAnswer.put("questionId", "question_id");
		assessmentAnswer.put("resourceGooruOId", "gooru_oid");
		assessmentAnswer.put("sequence", "sequence");
		assessmentAnswer.put("text", "answer_text");
		selectParam.put("assessmentAnswer", assessmentAnswer);
	}
	
	private void putClasspageUser(){
		
		Map<String, String> classpageUser = new HashMap<String,String>();
		classpageUser.put("userName", "username");
		classpageUser.put("gooruOId","classpage_gooru_oid");
		classpageUser.put("gooruUId", "gooru_uid");
		classpageUser.put("userGroupCode", "user_group_code");
		classpageUser.put("userGroupUId", "user_group_uid");
		classpageUser.put("organizationUId", "organization_uid");
		classpageUser.put("status", "active_flag");
		classpageUser.put("profileUrl", "profile_url");
		selectParam.put("classpageUser", classpageUser);
	}

	private void putOEResource(){
		
		Map<String, String> oeResource = new HashMap<String,String>();
		oeResource.put("userName", "username");
		oeResource.put("OEText", "~choice");
		oeResource.put("gooruUid", "gooruUid");
		oeResource.put("userGroupUId", "user_group_uid");
		oeResource.put("organizationUId", "organizationUid");
		oeResource.put("status", "active_flag");
		oeResource.put("answerObject", "~answer_object");
		oeResource.put("feedbackStatus", "~active");
		oeResource.put("feedbackText", "~feed_back");
		oeResource.put("feedbackProviderUId", "~feed_back_provider");
		oeResource.put("feedbackTimestamp", "~feed_back_timestamp");
		oeResource.put("hasFrameBreaker", "statistics.hasFrameBreakerN");
		selectParam.put("oeResource", oeResource);
	}
	
	private void putUserPreference(){
		
		Map<String, String> userPreference = new HashMap<String,String>();
		userPreference
				.put("category",
						"preferredCategory.exam,preferredCategory.handout,preferredCategory.interactive,preferredCategory.lesson,preferredCategory.slide,preferredCategory.textbook,preferredCategory.video,preferredCategory.website");
		userPreference
				.put("grade",
						"preferredGrade.1,preferredGrade.2,preferredGrade.3,preferredGrade.4,preferredGrade.5,preferredGrade.6,preferredGrade.7,preferredGrade.8,preferredGrade.9,preferredGrade.10,preferredGrade.11,preferredGrade.12,preferredGrade.higher education,preferredGrade.kindergarten,preferredGrade.high school");
		userPreference.put("subject",
				"preferredSubject.math, preferredSubject.science, preferredSubject.technology & engineering, preferredSubject.social sciences, preferredSubject.language arts");
		selectParam.put("userPreference", userPreference);
	}
	
	private void putUserPreferenceData(){
		
		Map<String, String> userPreferenceData = new HashMap<String,String>();
		userPreferenceData.put("subject", "subject.");
		userPreferenceData.put("course", "course.");
		userPreferenceData.put("unit", "unit.");
		userPreferenceData.put("topic", "topic.");
		userPreferenceData.put("lesson", "lesson.");
		selectParam.put("userPreferenceData", userPreferenceData);
	}
	
	public Map<String,String> getLiveDashboardData(String fields){
		Map<String, String> givenSelectValues = new HashMap<String, String>();
		for(String field : fields.split(",")){
			if(selectParam.get("liveDashboard").containsKey(field)){
			givenSelectValues.put(field, selectParam.get("liveDashboard").get(field));
			}
		}
		return givenSelectValues;
	}
	
	
	public String getClasspageCollectionOEResources(String fields, Map<String, String> selectValues) throws Exception {
		return getSelectValues(selectParam.get("classPageCollectionOEResource"), fields, selectValues);
	}
	
	public String getClasspageDetail(String fields,Map<String,String> selectValues) throws Exception {
		return getSelectValues(selectParam.get("classpageDetail"), fields, selectValues);
	}
	
	public String getClassExportDetail(String fields,Map<String,String> selectValues) throws Exception {
		return getSelectValues(selectParam.get("classExportDetail"), fields, selectValues);
	}

	public String getAggCollectionExportDetail(String fields,Map<String,String> selectValues) throws Exception {
		return getSelectValues(selectParam.get("collectionExportDetail"), fields, selectValues);
	}

	public String getClassResourceExportDetail(String fields,Map<String,String> selectValues) throws Exception {
		return getSelectValues(selectParam.get("classResourceExportDetail"), fields, selectValues);
	}

	public String getClasspageCollectionDetail(String fields,Map<String,String> selectValues) throws Exception {
		return getSelectValues(selectParam.get("classCollectionDetail"), fields, selectValues);
	}
	
	 /**
	  * classPage select values
	  */
	public String getClasspageCollectionUsage(String fields, Map<String, String> selectedValues) throws Exception {
		return getSelectValues(selectParam.get("classCollectionUsage"), fields, selectedValues);
	}
	
	public String getUser(String fields, Map<String, String> selectValues) throws Exception {
		return getSelectValues(selectParam.get("userDetail"), fields, selectValues);
	}

	 //classPage select values
	public String getClasspageResourceUsage(String fields, Map<String, String> selectedValues) throws Exception {
		return getSelectValues(selectParam.get("classpageResourceUsage"), fields, selectedValues);
	}
	
	public String getAssessmentAnswer(String fields,
			Map<String, String> selectValues) throws Exception {
		return getSelectValues(selectParam.get("assessmentAnswer"), fields, selectValues);
	}
		
		/**
		 * Will provide the classpage user list mapping
		 */
		public String getClasspageUser(String field,Map<String,String> selectedValues) throws Exception{
		return getSelectValues(selectParam.get("classpageUser"), field,selectedValues);
	}

	/**
	 * Provided Mapping between DB names with defined variables.
	 * Get the mapping DB names to fetch the OE resource & feedback details. 
	 * 
	 * @throws Exception
	 *             if requested fields are empty
	 */
	public String getOEResource(String fields,
			Map<String, String> selectedValues) throws Exception {
		return getSelectValues(selectParam.get("oeResource"), fields, selectedValues);
	}

	//Get user preference vectors column names
	public String getUserPreferenceData(String fields, Map<String, String> selectValues) throws Exception {

			return getSelectValues(selectParam.get("userPreference"), fields, selectValues);

	}
	
	public String getUserProficiencyData(String fields, Map<String, String> selectValues) throws Exception {
		return getSelectValues(selectParam.get("userPreferenceData"), fields, selectValues);
	}
	
	/**
	 * 
	 * @param hibernateSelectValues
	 *            Mapping database field name and variables
	 * @param value
	 *            User requested field values with comma separator
	 * @param selectedValues
	 *            Collection map used to set the user requested fields database
	 *            field name
	 * @return Returns requested field database field name
	 * @throws Exception If the requested field doesn't match with the database field name throws BadRequestException
	 */
	public String getSelectValues(Map<String, String> hibernateSelectValues,
			String value, Map<String, String> selectedValues) throws Exception {
		StringBuffer fetchFields = new StringBuffer();
		Set<String> filterFields = avoidDuplicate(value);
		for (String selectValue : filterFields) {
			if (hibernateSelectValues.containsKey(selectValue)) {
				selectedValues.put(selectValue,hibernateSelectValues.get(selectValue));
				addComma(fetchFields, hibernateSelectValues.get(selectValue));
			} else {
				throw new BadRequestException(ErrorMessages.E102 + selectValue);
			}
		}
		return fetchFields.toString();
	}
	
	/**
	 * To get the database field name with comma separator
	 */
	private void addComma(StringBuffer updatedCommaValues, String valueToAdd) {
		if (updatedCommaValues != null && updatedCommaValues.length() != 0) {
			updatedCommaValues.append(ApiConstants.COMMA);
		}
		updatedCommaValues.append(valueToAdd.trim());
	}

	private Set<String> avoidDuplicate(String valueToAdd){
		
		Set<String> seter = new LinkedHashSet<String>();
		        for(String data : valueToAdd.split(ApiConstants.COMMA)){
		        	seter.add(data);
		        }
		return seter;
	}
}
