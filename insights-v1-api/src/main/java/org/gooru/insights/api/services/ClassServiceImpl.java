package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.SessionAttributes;
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.constants.InsightsConstant;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.spring.exception.NotFoundException;
import org.gooru.insights.api.utils.DataUtils;
import org.gooru.insights.api.utils.InsightsLogger;
import org.gooru.insights.api.utils.ServiceUtils;
import org.gooru.insights.api.utils.ValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class ClassServiceImpl implements ClassService, InsightsConstant {
	
	private static final Logger logger = LoggerFactory.getLogger(ClassServiceImpl.class);
	
	@Autowired
	private BaseService baseService;

	@Autowired
	private CassandraService cassandraService;
	
	@Resource
	private Properties filePath;

	private BaseService getBaseService() {
		return baseService;
	}

	private CassandraService getCassandraService() {
		return cassandraService;
	}
	
	public ResponseParamDTO<Map<String,Object>> getAllStudentsUnitUsage(String classId, String courseId, String unitId, String studentId, String collectionType, Boolean getUsageData, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String,Object>> studentsMetaData = null;
		List<Map<String,Object>> resultData = new ArrayList<Map<String, Object>>();
		//validate ClassId
		isValidClass(classId);
		List<Map<String, Object>> lessonsRawData = getAssociatedItems(classId,unitId, null, true, isSecure, null, DataUtils.getResourceFields());
			
		responseParamDTO.setContent(lessonsRawData);
		
		//fetch usage data of lesson
		if(getUsageData) {
			
			//Get list of students
			studentsMetaData = getStudents(classId);

			if(!studentsMetaData.isEmpty() || StringUtils.isNotBlank(studentId)){
				
				if(StringUtils.isBlank(studentId)){
					studentId = getBaseService().getCommaSeparatedIds(studentsMetaData, ApiConstants.USER_UID).toString();
				}
				for(Map<String, Object> lessonrawData : lessonsRawData) {
	
					//fetch lesson usage data
					String lessonGooruOid = lessonrawData.get(ApiConstants.GOORUOID).toString();
					if(collectionType !=null){
						if(collectionType.equalsIgnoreCase(ApiConstants.ASSESSMENT)){
							collectionType = ApiConstants.ASSESSMENT_TYPES;
						}else if(collectionType.matches(ApiConstants.COLLECTION_MATCH)) {
							collectionType = ApiConstants.COLLECTION_MATCH;
						}
					}
					List<Map<String,Object>> contentsMetaData = getAssociatedItems(classId,lessonGooruOid,collectionType,true,isSecure,null,DataUtils.getResourceFields());
					lessonrawData.put(ApiConstants.ASSESSMENT_COUNT, contentsMetaData.size());
					if(!contentsMetaData.isEmpty()){
						StringBuffer collectionIds = ServiceUtils.getCommaSeparatedIds(contentsMetaData, ApiConstants.GOORUOID);
						String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
						Collection<String> rowKeys = ServiceUtils.generateCommaSeparatedStringToKeys(ApiConstants.TILDA,classLessonKey, studentId);
						Collection<String> columns = ServiceUtils.generateCommaSeparatedStringToKeys(ApiConstants.TILDA,collectionIds.toString(), DataUtils.getUnitProgressActivityFields().keySet());
						/**
						 * Get collection activity
						 */
						List<Map<String,Object>> contentUsage = getIdSeparatedMetrics(rowKeys,ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), columns, DataUtils.getUnitProgressActivityFields(), studentId,true,collectionIds.toString(),true);
						contentUsage = getBaseService().leftJoin(contentUsage,contentsMetaData,ApiConstants.GOORUOID,ApiConstants.GOORUOID);
						//group at content level
						contentUsage = getBaseService().groupRecordsBasedOnKey(contentUsage,ApiConstants.USER_UID,ApiConstants.USAGE_DATA);
						//Inject Lesson data
						contentUsage = getBaseService().injectMapRecord(contentUsage, lessonrawData);
						resultData.addAll(contentUsage);
					}
				}
				//group at user level
				resultData = getBaseService().groupRecordsBasedOnKey(resultData,ApiConstants.USER_UID,ApiConstants.USAGE_DATA);
				resultData = getBaseService().leftJoin(resultData,studentsMetaData,ApiConstants.USER_UID,ApiConstants.USER_UID);
				responseParamDTO.setContent(resultData);
			}
		}
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> getCoursePlan(String classId, String courseId, String userUid, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> unitDataMapAsList = new ArrayList<Map<String, Object>>();
		//validate ClassId
		isValidClass(classId);
		Long classGoal = getClassGoal(classId);
		List<Map<String, Object>> unitMetaDataAsList = getAssociatedItems(classId,courseId, null, true, isSecure, DataUtils.getResourceFields().keySet(), DataUtils.getResourceFields());
		for (Map<String, Object> unitMeta : unitMetaDataAsList) {
			List<Map<String, Object>> lessonDataMapAsList = new ArrayList<Map<String, Object>>();
			String unitGooruOid = unitMeta.get(ApiConstants.GOORUOID).toString();
			List<Map<String, Object>> lessonMetaDataAsList = getAssociatedItems(classId,unitGooruOid, null, true, isSecure, DataUtils.getResourceFields().keySet(), DataUtils.getResourceFields());
			for (Map<String, Object> lessonMetaData : lessonMetaDataAsList) {
				String lessonScoreStatus = null;
				String lessonGooruOid = lessonMetaData.get(ApiConstants.GOORUOID).toString();
				// Fetch lesson score for status
				String classLessonKey = getBaseService().appendTilda(classId, courseId, unitGooruOid, lessonGooruOid);
				if (StringUtils.isNotBlank(userUid)) {
					classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
				}
				OperationResult<ColumnList<String>> lessonMetricsData = getCassandraService().read(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey);
				ColumnList<String> lessonMetricColumnList = null;
				if (lessonMetricsData != null && !lessonMetricsData.getResult().isEmpty()) {
					lessonMetricColumnList = lessonMetricsData.getResult();
				}
				List<Map<String, Object>> itemMetaDataAsList = getAssociatedItems(classId,lessonGooruOid, null, true, isSecure, DataUtils.getResourceFields().keySet(), DataUtils.getResourceFields());
				for (Map<String, Object> itemMetaData : itemMetaDataAsList) {
					Long assessmentScore = null;
					String itemGooruOid = itemMetaData.get(ApiConstants.GOORUOID).toString();
					if (itemMetaData.get(ApiConstants.TYPE) != null && itemMetaData.get(ApiConstants.TYPE).toString().matches(ApiConstants.ASSESSMENT_TYPES)) {
						if (lessonMetricColumnList != null && lessonMetricColumnList.size() > 0) {
							assessmentScore = lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants._SCORE_IN_PERCENTAGE), null);
						if (assessmentScore == null && !(lessonScoreStatus != null && lessonScoreStatus.equals(ApiConstants.SCORE_MET))) {
							lessonScoreStatus = ApiConstants.NOT_ATTEMPTED;
						}else if(assessmentScore != null && (classGoal == 0  || assessmentScore >= classGoal) && !(lessonScoreStatus != null && lessonScoreStatus.equals(ApiConstants.NOT_ATTEMPTED))){
							lessonScoreStatus = ApiConstants.SCORE_MET;
						}else {
							lessonScoreStatus = ApiConstants.SCORE_NOT_MET;
							break;
						}
						}
					}
				}
				lessonMetaData.put(ApiConstants.SCORE_STATUS, lessonScoreStatus != null ? lessonScoreStatus : ApiConstants.NOT_ATTEMPTED);
				lessonDataMapAsList.add(lessonMetaData);
			}
			unitMeta.put(ApiConstants.ITEM, lessonDataMapAsList);
			unitDataMapAsList.add(unitMeta);
		}
		responseParamDTO.setContent(unitDataMapAsList);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String, Object>> getUnitPlan(String classId, String courseId, String unitId, String userUid, boolean isSecure) throws Exception {

		List<Map<String, Object>> lessonDataMapAsList = new ArrayList<Map<String, Object>>();
		//validate ClassId
		isValidClass(classId);
		Long classMinScore = getClassGoal(classId);
		List<Map<String,Object>> lessonData = getAssociatedItems(classId,unitId, null, true, isSecure, null, DataUtils.getResourceFields());
		for (Map<String,Object> lesson : lessonData) {
			
			long notAttempted = 0,assessmentCount = 0;
			List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
			String lessonGooruOid = lesson.get(ApiConstants.GOORUOID).toString();
			Map<String,String> assessmentAliesName = DataUtils.getResourceFields();
			assessmentAliesName.put(ApiConstants.URL, ApiConstants.URL);
			List<Map<String,Object>> itemData = getAssociatedItems(classId,lessonGooruOid, null, true, isSecure, null, DataUtils.getResourceFields());
			String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
			}
			//Fetch lesson metrics
			OperationResult<ColumnList<String>> lessonMetricsData = getCassandraService().read(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey);
			ColumnList<String> lessonMetricColumnList = lessonMetricsData != null ? lessonMetricsData.getResult() : null;
			//Fetch item data
			String lessonScoreStatus = null;
			String collectionViewStatus = null;
			for (Map<String,Object> item : itemData) {

				String itemGooruOid = item.get(ApiConstants.GOORUOID).toString();
				String itemType = item.get(ApiConstants.TYPE) != null ? item.get(ApiConstants.TYPE).toString() : ApiConstants.STRING_EMPTY;
				long itemViews = lessonMetricColumnList != null  ? lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid,ApiConstants.VIEWS), 0L) : 0;
				String itemScoreStatus = null;
                if (itemType.matches(ApiConstants.ASSESSMENT_TYPES)) {
                	
                	assessmentCount++;
					Long assessmentScore = lessonMetricColumnList != null ? lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid,ApiConstants._SCORE_IN_PERCENTAGE), null) : null;
					if(assessmentScore == null) {
						notAttempted++;
						itemScoreStatus = ApiConstants.NOT_ATTEMPTED;
					} else if(assessmentScore >= classMinScore) {
						itemScoreStatus = ApiConstants.SCORE_MET;
					}else {
						itemScoreStatus = ApiConstants.SCORE_NOT_MET;
						lessonScoreStatus = ApiConstants.SCORE_NOT_MET;
					}
				}else if(itemViews > 0) {
					itemScoreStatus = ApiConstants.VIEWED;
				}else {
					itemScoreStatus = ApiConstants.NOT_VIEWED;
					collectionViewStatus = ApiConstants.NOT_VIEWED;
				}
				item.put(ApiConstants.VIEWS, itemViews);					
				item.put(ApiConstants.SCORE_STATUS, itemScoreStatus != null ? itemScoreStatus : ApiConstants.NOT_ATTEMPTED);
				itemDataMapAsList.add(item);
			}
			lesson.put(ApiConstants.ITEM, itemDataMapAsList);
			if (lessonScoreStatus != null || (notAttempted != 0 && notAttempted < assessmentCount)) {
				lessonScoreStatus = ApiConstants.SCORE_NOT_MET;
			} else if (assessmentCount == 0 && itemData.size() > 0) {
				// Only For collection
				if (collectionViewStatus == null) {
					lessonScoreStatus = ApiConstants.VIEWED;
				} else {
					lessonScoreStatus = collectionViewStatus;
				}
			} else if (notAttempted == assessmentCount) {
				lessonScoreStatus = ApiConstants.NOT_ATTEMPTED;
			} else {
				lessonScoreStatus = ApiConstants.SCORE_MET;
			}
			lesson.put(ApiConstants.SCORE_STATUS, lessonScoreStatus);
			lesson.put(ApiConstants.TYPE, ApiConstants.LESSON);
			lessonDataMapAsList.add(lesson);
		}
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		responseParamDTO.setContent(lessonDataMapAsList);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> getCourseProgress(String classId, String courseId, String userUid, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = null;
		//validate ClassId
		isValidClass(classId);
		if (StringUtils.isNotBlank(userUid)) {
			responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
			List<Map<String,Object>> unitItemsMetaData = getAssociatedItems(classId,courseId, null, true, isSecure, null, DataUtils.getResourceFields());
			for(Map<String,Object> unitItem : unitItemsMetaData) {
				String unitGooruOid = unitItem.get(ApiConstants.GOORUOID).toString();
				String classUnitKey = getBaseService().appendTilda(classId, courseId, unitGooruOid,userUid);
				// Fetch unit metadata
				getContentMeta(baseService.appendTilda(classId,unitGooruOid), getBaseService().appendComma(ApiConstants.ASSESSMENT_COUNT,ApiConstants.COLLECTION_COUNT), unitItem);
				long collectionsViewedInUnit = 0, unitCollectionsTotalStudyTime = 0,unitAvgScore = 0,assessmentsAttemptedInUnit = 0;
				// Fetch unit's total study time & unique views of collections 
				OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
						getBaseService().appendTilda(classUnitKey, ApiConstants.COLLECTION));
				if (collectionMetricsData != null && collectionMetricsData.getResult() != null) {
					ColumnList<String> collectionMetrics = collectionMetricsData.getResult();
						collectionsViewedInUnit = collectionMetrics.getLongValue(ApiConstants._UNIQUE_VIEWS, 0L);
						unitCollectionsTotalStudyTime = collectionMetrics.getLongValue(ApiConstants._TIME_SPENT, 0L);
				}
				// Fetch unit's avgScore & unique attempt count of assessments 
				OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), getBaseService().appendTilda(classUnitKey, ApiConstants.ASSESSMENT));
				if (assessmentMetricsData != null && assessmentMetricsData.getResult() != null) {
					ColumnList<String> assessmentMetricColumnList = assessmentMetricsData.getResult();
					assessmentsAttemptedInUnit = assessmentMetricColumnList.getLongValue(ApiConstants._UNIQUE_VIEWS, 0L);
					unitAvgScore = assessmentMetricColumnList.getLongValue(ApiConstants._SCORE_IN_PERCENTAGE, 0L);
				}
				unitItem.put(ApiConstants.COLLECTIONS_VIEWED, collectionsViewedInUnit);
				unitItem.put(ApiConstants.TOTAL_STUDY_TIME, unitCollectionsTotalStudyTime);
				unitItem.put(ApiConstants.ASSESSMENTS_ATTEMPTED, assessmentsAttemptedInUnit);
				unitItem.put(ApiConstants.SCORE_IN_PERCENTAGE, unitAvgScore);
			}
			responseParamDTO.setContent(unitItemsMetaData);
		}else{
			responseParamDTO = getAllStudentProgressByUnit(classId, courseId, isSecure);	
		}
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getUserSessions(String classId, String courseId, String unitId, String lessonId, String collectionId, String collectionType,
			String userUid, boolean fetchOpenSession, boolean isSecure) throws Exception {
		String key = null;
		//validate ClassId
		isValidClass(classId);
		if (StringUtils.isNotBlank(collectionId) && StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(classId) && StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(unitId)
				&& StringUtils.isNotBlank(lessonId)) {
			key = baseService.appendTilda(classId, courseId, unitId, lessonId, collectionId, userUid);
		} else if (StringUtils.isNotBlank(collectionId) && StringUtils.isNotBlank(userUid)) {
			key = baseService.appendTilda(collectionId, userUid);
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E106);
		}
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> resultSet = getSessionInfo(key, collectionType,fetchOpenSession);
		resultSet = ServiceUtils.sortBy(resultSet, EVENT_TIME, ApiConstants.ASC);
		responseParamDTO.setContent(addSequence(resultSet));
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> addSequence(List<Map<String, Object>> resultSet) {
		List<Map<String, Object>> finalSet = null;
		if (resultSet != null) {
			int sequence = 1;
			finalSet = new ArrayList<Map<String, Object>>();
			for (Map<String, Object> resultMap : resultSet) {
				resultMap.put(SEQUENCE, sequence++);
				finalSet.add(resultMap);
			}
		}
		return finalSet;
	}

	private Map<String,Object> generateSessionMap(String sessionId,Long eventTime){
		HashMap<String, Object> session = new HashMap<String, Object>();
		session.put(SESSION_ID, sessionId);
		session.put(EVENT_TIME, eventTime);
		return session;
	}
	
	private List<Map<String, Object>> getSessionInfo(String key, String collectionType, boolean openSession) {
		OperationResult<ColumnList<String>> sessions = getCassandraService().read(ColumnFamily.SESSION.getColumnFamily(), key);
		OperationResult<ColumnList<String>>  sessionsOperationalInfo = getCassandraService().read(ColumnFamily.SESSION.getColumnFamily(), ServiceUtils.appendTilda(key, INFO));
		List<Map<String,Object>> sessionList = new ArrayList<Map<String,Object>>();
		if( sessions != null && sessions.getResult() != null && sessionsOperationalInfo != null && sessionsOperationalInfo.getResult() != null) {
			String type = STOP;
			if(collectionType.equalsIgnoreCase(ASSESSMENT)){
				type = openSession ? START : STOP;
			}
			ColumnList<String> sessionResult = sessions.getResult();
			ColumnList<String> sessionInfo = sessionsOperationalInfo.getResult();
			for (Column<String> sessionColumn : sessionResult) {
					if (collectionType.equalsIgnoreCase(ASSESSMENT) && sessionInfo.getStringValue(baseService.appendTilda(sessionColumn.getName(), TYPE), ApiConstants.STRING_EMPTY).equalsIgnoreCase(type)) {
						sessionList.add(generateSessionObject(sessionColumn, sessionInfo, openSession));
					}else if(collectionType.equalsIgnoreCase(COLLECTION)){
						sessionList.add(generateSessionObject(sessionColumn, sessionInfo, openSession));						
					}
			}
		}
		return sessionList;
	}
	
	public ResponseParamDTO<Map<String,Object>> getUnitProgress(String classId, String courseId, String unitId, String userUid, boolean isSecure) throws Exception {
		
		//validate ClassId
		isValidClass(classId);
		//Fetch minScore of class
		Long classMinScore = getClassGoal(classId);
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String,Object>> lessonsMetaData = getAssociatedItems(classId,unitId, null, true, isSecure, null, DataUtils.getResourceFields());
		for (Map<String,Object> lessonDataAsMap : lessonsMetaData) {
			
			long assessmentCount = 0, notAttempted = 0;
			String lessonScoreStatus = null,collectionViewStatus = null;
			String lessonGooruOid = lessonDataAsMap.get(ApiConstants.GOORUOID).toString();
			String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
			}
			List<Map<String,Object>> itemData = getAssociatedItems(classId,lessonGooruOid, null, true, isSecure, null, DataUtils.getResourceFields());

			OperationResult<ColumnList<String>> lessonMetricsData = getClassMetricsForItem(classLessonKey,lessonDataAsMap, null, DataUtils.getUnitProgressActivityFields(), isSecure);
			ColumnList<String> lessonMetricColumnList = (lessonMetricsData != null && lessonMetricsData.getResult() != null) ? lessonMetricsData.getResult() : null;
			for (Map<String,Object> item : itemData) {

				String itemGooruOid = item.get(ApiConstants.GOORUOID).toString();
				long itemViews = 0,timeSpent = 0;
				Long assessmentScore = null;
				if(lessonMetricColumnList != null) {
					itemViews = lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid,ApiConstants.VIEWS), 0L);
					assessmentScore = lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid,ApiConstants._SCORE_IN_PERCENTAGE), null);
					timeSpent = lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants._TIME_SPENT), 0L);
				}
				item.put(ApiConstants.VIEWS, itemViews);
				item.put(ApiConstants.TIMESPENT, timeSpent);
				String itemType = item.get(ApiConstants.TYPE) != null ? item.get(ApiConstants.TYPE).toString() : ApiConstants.STRING_EMPTY;
                if (itemType.matches(ApiConstants.ASSESSMENT_TYPES)) {
                	assessmentCount++;
					if(assessmentScore == null) {
						notAttempted++;
					} else if(assessmentScore >= classMinScore) {
					}else {
						lessonScoreStatus = ApiConstants.SCORE_NOT_MET;
					}
				}else if(itemViews == 0) {
					collectionViewStatus = ApiConstants.NOT_VIEWED;
				}
                item.put(ApiConstants.SCORE_IN_PERCENTAGE, assessmentScore != null ? assessmentScore : 0L);
			}
			lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);
			lessonDataAsMap.put(ApiConstants.ITEM, itemData);
			if (lessonScoreStatus != null || (notAttempted != 0 && notAttempted < assessmentCount)) {
				lessonScoreStatus = ApiConstants.SCORE_NOT_MET;
			} else if (assessmentCount == 0 && itemData.size() > 0) {
				// Only For collection
				if (collectionViewStatus == null) {
					lessonScoreStatus = ApiConstants.VIEWED;
				} else {
					lessonScoreStatus = collectionViewStatus;
				}
			} else if (notAttempted == assessmentCount) {
				lessonScoreStatus = ApiConstants.NOT_ATTEMPTED;
			} else {
				lessonScoreStatus = ApiConstants.SCORE_MET;
			}
			lessonDataAsMap.put(ApiConstants.SCORE_STATUS, lessonScoreStatus);
		}
		responseParamDTO.setContent(lessonsMetaData);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> getLessonAssessmentsUsage(String classId, String courseId, String unitId, String lessonId, String assessmentIds, String userUid, boolean isSecure) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		//validate ClassId
		isValidClass(classId);
		String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonId);
		if (StringUtils.isNotBlank(userUid)) {
			classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
		}
		responseParamDTO.setContent(getClassMetricsForAllItems(classLessonKey, assessmentIds, DataUtils.getLessonPlanClassActivityFields(), isSecure));
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String, Object>> getStudentAssessmentData(String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid,
			String collectionType, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
		Map<String, Object> itemDetailAsMap = new HashMap<String, Object>();
		OperationResult<ColumnList<String>> itemsColumnList = null;
		Long classMinScore = 0L; String sessionKey = null;
		Long scoreInPercentage = 0L; Long score = 0L; String evidence = null; Long timespent = 0L; Long scorableCountOnEvent = 0L; Long totalReaction = 0L; Long reactedCount = 0L; Long avgReaction = 0L;
		//validate ClassId
		isValidClass(classId);
		if ((sessionId != null && StringUtils.isNotBlank(sessionId.trim()))) {
			sessionKey = sessionId;
		} else if ((classId != null && StringUtils.isNotBlank(classId.trim())) && (courseId != null && StringUtils.isNotBlank(courseId.trim())) 
				&& (unitId != null && StringUtils.isNotBlank(unitId.trim())) && (lessonId != null && StringUtils.isNotBlank(lessonId.trim()))
				&& (userUid != null && StringUtils.isNotBlank(userUid.trim()))) {
			sessionKey = getSessionIdFromKey(getBaseService().appendTilda(SessionAttributes.RS.getSession(), classId, courseId, unitId, lessonId, assessmentId, userUid));
		} else if((userUid != null && StringUtils.isNotBlank(userUid.trim()))) {
			sessionKey = getSessionIdFromKey(getBaseService().appendTilda(SessionAttributes.RS.getSession(), assessmentId, userUid));
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E111, getBaseService().appendComma("contentGooruId", "sessionId")
					, getBaseService().appendComma("contentGooruId", "userUid"));
		}
		
		// Fetch goal for the class
		if (classId != null && StringUtils.isNotBlank(classId.trim())) {
			OperationResult<ColumnList<String>> classData = getCassandraService().read(ColumnFamily.CLASS.getColumnFamily(), classId);
			if (classData != null && !classData.getResult().isEmpty() && classData.getResult().size() > 0) {
				classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
			}
		}
		
		//Fetch score and evidence of assessment
		if (StringUtils.isNotBlank(sessionKey)) {
			itemsColumnList = getCassandraService().read(ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionKey);
			if (itemsColumnList != null && !itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
				ColumnList<String> lessonMetricColumns = itemsColumnList.getResult();
				score = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants.SCORE), 0L);
				scoreInPercentage = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._SCORE_IN_PERCENTAGE), 0L);
				evidence = lessonMetricColumns.getStringValue(getBaseService().appendTilda(assessmentId, ApiConstants.EVIDENCE), null);
				timespent = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._TIME_SPENT), 0L);
				scorableCountOnEvent = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._QUESTION_COUNT), 0L);
				totalReaction = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._TOTAL_REACTION), 0L);
				reactedCount = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._REACTED_COUNT), 0L);
				if(reactedCount != 0L) {
					avgReaction = totalReaction/reactedCount;
				}
			} else {
				logger.info("No session available for key" + sessionKey);
			}
		}
		
		//Fetch assessment metadata
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		resourceColumns.add(ApiConstants.FOLDER);
		getResourceMeta(itemDetailAsMap, isSecure,assessmentId, resourceColumns);

		
		// Fetch assessment count
		Long questionCount = 0L; Long scorableQuestionCount = 0L; Long oeCount = 0L; Long resourceCount = 0L; Long itemCount = 0L; 
		Map<String, Long> contentMetaAsMap = getContentMeta(assessmentId, getBaseService().appendComma(ApiConstants.QUESTION_COUNT, ApiConstants._OE_COUNT, ApiConstants.RESOURCE_COUNT, ApiConstants._ITEM_COUNT));
		if (!contentMetaAsMap.isEmpty()) {
			questionCount = (contentMetaAsMap.containsKey(ApiConstants.QUESTION_COUNT) && contentMetaAsMap.get(ApiConstants.QUESTION_COUNT) != null) ? contentMetaAsMap.get(ApiConstants.QUESTION_COUNT) : 0L;
			oeCount = (contentMetaAsMap.containsKey(ApiConstants._OE_COUNT) && contentMetaAsMap.get(ApiConstants._OE_COUNT) != null) ? contentMetaAsMap.get(ApiConstants._OE_COUNT) : 0L;
			resourceCount = (contentMetaAsMap.containsKey(ApiConstants.RESOURCE_COUNT) && contentMetaAsMap.get(ApiConstants.RESOURCE_COUNT) != null) ? contentMetaAsMap.get(ApiConstants.RESOURCE_COUNT) : 0L;
			itemCount = (contentMetaAsMap.containsKey(ApiConstants._ITEM_COUNT) && contentMetaAsMap.get(ApiConstants._ITEM_COUNT) != null) ? contentMetaAsMap.get(ApiConstants._ITEM_COUNT) : 0L;
			if (questionCount > 0) {
				scorableQuestionCount = questionCount - oeCount;
			}
		}
		itemDetailAsMap.put(ApiConstants.QUESTION_COUNT, questionCount);
		itemDetailAsMap.put(ApiConstants.OE_COUNT, oeCount);
		itemDetailAsMap.put(ApiConstants.SCORABLE_QUESTION_COUNT, scorableQuestionCount);
		itemDetailAsMap.put(ApiConstants.RESOURCE_COUNT, resourceCount);
		itemDetailAsMap.put(ApiConstants.ITEM_COUNT, itemCount);
		itemDetailAsMap.put(ApiConstants.SCORABLE_COUNT_ON_EVENT, scorableCountOnEvent);
		itemDetailAsMap.put(ApiConstants.SESSIONID, sessionKey);
		itemDetailAsMap.put(ApiConstants.AVG_REACTION, avgReaction);
		//Fetch username and profile url
		String username = null;
		OperationResult<ColumnList<String>> userColumnList = getCassandraService().read(ColumnFamily.USER.getColumnFamily(), userUid);
		if (!userColumnList.getResult().isEmpty() && userColumnList.getResult().size() > 0) {
			username = userColumnList.getResult().getStringValue(ApiConstants.USERNAME, null);
		}
		
		//Get sessions 
		ResponseParamDTO<Map<String, Object>> sessionResponse = getUserSessions(classId, courseId, unitId, lessonId, assessmentId, ApiConstants.ASSESSMENT, userUid, false, isSecure);
		
		itemDetailAsMap.put(ApiConstants.GOORUOID, assessmentId);
		itemDetailAsMap.put(ApiConstants.USERNAME, username);
		itemDetailAsMap.put(ApiConstants.PROFILE_URL, filePath.getProperty(ApiConstants.USER_PROFILE_URL_PATH).replaceAll(ApiConstants.ID, userUid));
		itemDetailAsMap.put(ApiConstants.GOAL, classMinScore);
		itemDetailAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, scoreInPercentage);
		itemDetailAsMap.put(ApiConstants.EVIDENCE, evidence);
		itemDetailAsMap.put(ApiConstants.SCORE, score);
		itemDetailAsMap.put(ApiConstants.TIMESPENT, timespent);
		itemDetailAsMap.put(ApiConstants.SESSION, sessionResponse.getContent());
		itemDetailAsMap.put(ApiConstants.AVG_REACTION, avgReaction);
		itemDataMapAsList.add(itemDetailAsMap);
		responseParamDTO.setContent(itemDataMapAsList);
		return responseParamDTO;
	}
	
	private Map<String, Long> getContentMeta(String key, String fetchColumnList) {
		Map<String, Long> contentMetaAsMap = new HashMap<String, Long>();
		OperationResult<ColumnList<String>> contentMetaColumnList = getCassandraService().read(ColumnFamily.CONTENT_META.getColumnFamily(), key);
		if (!contentMetaColumnList.getResult().isEmpty() && contentMetaColumnList.getResult().size() > 0) {
			ColumnList<String> contentMetaColumns = contentMetaColumnList.getResult();
			for(String columnNameTofetch : fetchColumnList.split(ApiConstants.COMMA)) {
				contentMetaAsMap.put(columnNameTofetch, contentMetaColumns.getLongValue(columnNameTofetch, null));
			}
		}
		return contentMetaAsMap;
	}
	private String getSessionIdFromKey(String recentSessionKey) {
		String recentSessionId = null;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(ColumnFamily.SESSION.getColumnFamily(), getBaseService().appendTilda(recentSessionKey));
		if(!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			recentSessionId = itemsColumnList.getResult().getStringValue(ApiConstants._SESSION_ID, null);
		}
		return recentSessionId;
	}
	
	private List<Map<String, Object>> getResourceData(boolean isSecure, String keys, Collection<String> columnsToFetch, String type) {
		List<Map<String, Object>> rawDataMapAsList = getBaseService().getRowsColumnValues(getCassandraService().readAll(ColumnFamily.RESOURCE.getColumnFamily(), null, keys, new String(), columnsToFetch));
		Map<String, Map<Integer, String>> combineMap = new HashMap<String, Map<Integer, String>>();
		Map<Integer, String> filterMap = new HashMap<Integer, String>();
		filterMap.put(0, filePath.getProperty(ApiConstants.NFS_BUCKET));
		if(type.equalsIgnoreCase(ApiConstants.RESOURCE)) {
			filterMap.put(1, ApiConstants.FOLDER);
			filterMap.put(2, ApiConstants.THUMBNAIL);
		} else {
			filterMap.put(1, ApiConstants.THUMBNAIL);
		}
		combineMap.put(ApiConstants.THUMBNAIL, filterMap);
		rawDataMapAsList = getBaseService().appendInnerData(rawDataMapAsList, combineMap, isSecure ? ApiConstants.HTTPS : ApiConstants.HTTP);
		rawDataMapAsList = getBaseService().addCustomKeyInMapList(rawDataMapAsList, ApiConstants.GOORUOID, null);
		return rawDataMapAsList;
	}

	private void getResourceMeta(Map<String, Object> dataMap, boolean isSecure, String key, Collection<String> columnsToFetch) {
		//Set default
		for (String column : columnsToFetch) {
			if (column.equalsIgnoreCase("question.type") || column.equalsIgnoreCase("question.questionType") || column.equalsIgnoreCase("resourceType")) {
				dataMap.put(ApiConstants.TYPE, null);
			} else if(column.equalsIgnoreCase(ApiConstants.FOLDER)) { 
				continue;
			} else{
				dataMap.put(column, null);
			}
		}

		// fetch metadata
		ColumnList<String> resourceColumn = getCassandraService().read(ColumnFamily.RESOURCE.getColumnFamily(), key, columnsToFetch).getResult();
		// form thumbnail
		if (columnsToFetch.contains(ApiConstants.THUMBNAIL)) {
			String thumbnail = resourceColumn.getStringValue(ApiConstants.THUMBNAIL, null);
			String formedThumbnail = null;
			if (StringUtils.isNotBlank(thumbnail)) {
				String nfsPath = filePath.getProperty(ApiConstants.NFS_BUCKET);
				String folder = resourceColumn.getStringValue(ApiConstants.FOLDER, ApiConstants.STRING_EMPTY);
				String protocol = isSecure ? ApiConstants._HTTPS : ApiConstants._HTTP;
				if (thumbnail.startsWith(ApiConstants._HTTP)) {
					formedThumbnail = thumbnail;
					if(isSecure) {
						formedThumbnail = formedThumbnail.replaceFirst(ApiConstants._HTTP, ApiConstants._HTTPS);
					}
				} else {
					if (thumbnail.contains(folder)) {
						formedThumbnail = getBaseService().appendForwardSlash(protocol,nfsPath, thumbnail);
					} else {
						formedThumbnail = getBaseService().appendForwardSlash(protocol,nfsPath, folder, thumbnail);
					}
				}
			}
			dataMap.put(ApiConstants.THUMBNAIL, formedThumbnail);
		}

		for (Column<String> column : resourceColumn) {
			if (column.getName().equals(ApiConstants.RESOURCE_TYPE) || column.getName().equalsIgnoreCase("question.type") || column.getName().equalsIgnoreCase("question.questionType")) {
				dataMap.put(ApiConstants.TYPE, column.getStringValue());
			} else if (column.getName().equals(ApiConstants.THUMBNAIL) || column.getName().equals(ApiConstants.FOLDER)) {
				continue;
			} else {
				dataMap.put(column.getName(), column.getStringValue());
			}
		}
	}

	public List<Map<String,Object>> getResourcesMetaData(Collection<String> keys,Collection<String> resourceColumns,String type,Map<String,String> aliesNames, boolean isSecure) {

		OperationResult<Rows<String, String>> resourceRows = getCassandraService().readAll(ColumnFamily.RESOURCE.getColumnFamily(), keys, resourceColumns);
		List<Map<String,Object>> resourceMetaList = new ArrayList<Map<String,Object>>();
		Map<String,List<String>> mergeResourceDualColumnValues = DataUtils.getMergeDualColumnValues().get(ColumnFamily.RESOURCE.getColumnFamily());
		for(Row<String,String> row : resourceRows.getResult()){
			if(type != null){
				String resourceType = row.getColumns().getStringValue(ApiConstants.RESOURCE_TYPE, ApiConstants.STRING_EMPTY);
				if(!resourceType.matches(type)){
						continue;
				}
			}
			Map<String,Object> resourceMetaData = DataUtils.getColumnFamilyContent(ColumnFamily.RESOURCE.getColumnFamily(), row.getColumns(), aliesNames, null, resourceColumns, mergeResourceDualColumnValues,isSecure);
			resourceMetaList.add(resourceMetaData);
		}
		return resourceMetaList;
	}
	
	private Map<String, Object> getClassMetricsAsMap(String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		Long views = 0L; Long timeSpent = 0L; Long score = 0L; 
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			views = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants.VIEWS), 0L);
			timeSpent = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants._TIME_SPENT), 0L);
			score = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants._SCORE_IN_PERCENTAGE), 0L);
		}
		usageAsMap.put(ApiConstants.VIEWS, views);
		usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
		usageAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, score);
		return usageAsMap;
	}
	
	public void getResourceMetaData(Map<String, Object> dataMap,String type, String key,Map<String,String> aliesNames, boolean isSecure) {
        // fetch metadata
        Collection<String> resourceColumns = new ArrayList<String>();
        resourceColumns.add(ApiConstants.TITLE);
        resourceColumns.add(ApiConstants.RESOURCE_TYPE);
        ColumnList<String> resourceColumn = getCassandraService().read(ColumnFamily.RESOURCE.getColumnFamily(), key, resourceColumns).getResult();
        if(type != null){
                String resourceType = resourceColumn.getStringValue(ApiConstants.RESOURCE_TYPE, ApiConstants.STRING_EMPTY);
                if(!resourceType.matches(type)){
                                return;
                }
        }
		Map<String,List<String>> mergeResourceDualColumnValues = DataUtils.getMergeDualColumnValues().get(ColumnFamily.RESOURCE.getColumnFamily());
		dataMap = DataUtils.getColumnFamilyContent(ColumnFamily.RESOURCE.getColumnFamily(), resourceColumn, aliesNames, null,resourceColumns, mergeResourceDualColumnValues, isSecure);
	}
	
	private List<Map<String, Object>> getClassMetricsForAllItems(String key, String contentGooruOids, Map<String,String> aliesName, boolean isSecure) {
		List<Map<String, Object>> usageAsMapAsList = new ArrayList<Map<String, Object>>();;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if(StringUtils.isNotBlank(contentGooruOids)) {
			for (String itemGooruOid : contentGooruOids.split(ApiConstants.COMMA)) {
				Map<String, Object> usageAsMap = new HashMap<String, Object>();
				usageAsMap.put(ApiConstants.GOORUOID, itemGooruOid);
				if(itemsColumnList != null && itemsColumnList.getResult() != null) {
					usageAsMap.putAll(DataUtils.getColumnFamilyContent(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), itemsColumnList.getResult(), aliesName,itemGooruOid, aliesName.keySet(), null, isSecure));
				}else {
					DataUtils.fetchDefaultData(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), aliesName, usageAsMap);
				}
				if (!usageAsMap.isEmpty()) {
					usageAsMapAsList.add(usageAsMap);
				}
			}
		}
		return usageAsMapAsList;
	}

	private OperationResult<ColumnList<String>> getClassMetricsForItem(String key,Map<String, Object> usageAsMap, String columnPrefix, Map<String,String> aliesName, boolean isSecure) {
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if(itemsColumnList != null && itemsColumnList.getResult() != null) {
			usageAsMap.putAll(DataUtils.getColumnFamilyContent(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), itemsColumnList.getResult(), aliesName,columnPrefix, aliesName.keySet(), null, isSecure));
		}else {
			DataUtils.fetchDefaultData(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), aliesName, usageAsMap);
		}
		return itemsColumnList;
	}
	
	private Map<String, Object> getActivityMetricsAsMap(String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
		Long views = 0L; Long timeSpent = 0L; Long score = 0L;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			views = itemMetricColumns.getLongValue(ApiConstants.VIEWS, 0L);
			timeSpent = itemMetricColumns.getLongValue(ApiConstants._TIME_SPENT, 0L);
			score = itemMetricColumns.getLongValue(ApiConstants._SCORE_IN_PERCENTAGE, 0L);
		}
		usageAsMap.put(ApiConstants.VIEWS, views);
		usageAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, score);
		usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
		return usageAsMap;
	}
	
	private List<Map<String,Object>> getDirectActivityMetrics(Collection<String> rowKeys,String columnFamily, String studentIds,boolean isUserIdInKey,String contentIds, boolean userProcess, Map<String,String> aliesNames, boolean isSecure) {
		Collection<String> fetchedContentIds = new ArrayList<String>();
		List<Map<String,Object>> contentUsageData = new ArrayList<Map<String,Object>>();
		Map<String,Set<String>> studentContentMapper = new HashMap<String,Set<String>>();

		/**
		 * Get Activity data
		 */
		OperationResult<Rows<String, String>> activityData = getCassandraService().readAll(columnFamily, rowKeys, aliesNames.keySet());
		if (activityData != null && activityData.getResult() != null) {
			
			Rows<String, String> itemMetricRows = activityData.getResult();
			//Iterate for Every Row
			for(Row<String, String> metricRow : itemMetricRows){
				String userId = null;
				String contentId = null;
				Map<String,Object> usageMap = new HashMap<String,Object>();
				//Get content Id
				for(String id : contentIds.split(ApiConstants.COMMA)){
					if(metricRow.getKey().contains(id)){
						contentId = id;
						break;
					}
				}
				usageMap.put(ApiConstants.GOORUOID, contentId);
				//Get the metric data
				usageMap.putAll(DataUtils.getColumnFamilyContent(columnFamily, metricRow.getColumns(), aliesNames, null, aliesNames.keySet(), null, isSecure));
				//Get the userId for the content usage,If we need user level tril down
				if(userProcess){
					userId = includeUserId(userProcess,isUserIdInKey,studentIds,userId,contentId,metricRow,usageMap,studentContentMapper);
				}
				//Storing the Fetched content id to support including default value at content level 
				if(!fetchedContentIds.contains(contentId) && contentId.length() > 35){
					fetchedContentIds.add(contentId);
				}
				contentUsageData.add(usageMap);
			}
		}
		/**
		 * Set default value at user-content level
		 */
		if(userProcess){
			insertDefaultUserContents(columnFamily, contentIds, studentContentMapper, studentIds, aliesNames,contentUsageData);
		}else {
			/**
			 * Set default value only at collection level
			 */
			for(String id : contentIds.split(ApiConstants.COMMA)){
				if(!fetchedContentIds.contains(id)){
					Map<String,Object> dataMap = new HashMap<String, Object>();
					DataUtils.fetchDefaultData(columnFamily, aliesNames, dataMap);
					dataMap.put(ApiConstants.GOORUOID, id);
					contentUsageData.add(dataMap);
				}
			}
		}
		return contentUsageData;
	}
	
	/**
	 * @param rowKeys is the list of CF rowKeys
	 * @param columnFamily is the name of the columnFamily
	 * @param aliesName is the CF column Names
	 * @param studentIds List of students ids this may be null if we don't want user level break down
	 * @param isUserIdInKey check is the user id available in rowkey
	 * @param contentIds is the list of itemIds
	 * @param userProcess check are we need to process at user level
	 */
	private List<Map<String,Object>> getIdSeparatedMetrics(Collection<String> rowKeys,String columnFamily,Collection<String> columnNames, Map<String, String> aliesNames, String studentIds,boolean isUserIdInKey,String contentIds, boolean userProcess) {

		Collection<String> fetchedContentIds = new ArrayList<String>();
		List<Map<String,Object>> contentUsageData = new ArrayList<Map<String,Object>>();
		Map<String,Set<String>> studentContentMapper = new HashMap<String,Set<String>>();

		/**
		 * Get Activity data
		 */
		OperationResult<Rows<String, String>> activityData = getCassandraService().readAll(columnFamily, rowKeys, columnNames);
		if (activityData != null && activityData.getResult() != null) {
			
			Rows<String, String> itemMetricRows = activityData.getResult();
			//Iterate for Every Row
			for(Row<String, String> metricRow : itemMetricRows){
				String userId = null;
				Map<String,Map<String, Object>> idBasedContentUsage = new HashMap<String,Map<String, Object>>();
				ColumnList<String> columnList = metricRow.getColumns();
				//Iterate for Fetched column
				for(String column : columnNames){
					Map<String,Object> usageMap = new HashMap<String,Object>();
					String[] columnMetaInfo = column.split(ApiConstants.TILDA);
					String metricName = (columnMetaInfo.length > 1) ? columnMetaInfo[columnMetaInfo.length-1] : columnMetaInfo[0];
					//No Need to process for MA question A,B,C,D option
					if((columnMetaInfo.length > 1 ? columnMetaInfo[1].matches(ApiConstants.OPTIONS_MATCH) : columnMetaInfo[0].matches(ApiConstants.OPTIONS_MATCH))){
						continue;
					}
					//Get the metric data
					DataUtils.fetchData(columnFamily, DataUtils.getColumnFamilyDataTypes().get(columnFamily), columnMetaInfo[0], metricName, aliesNames.get(metricName), columnList, usageMap);
					//Get the userId for the content usage,If we need user level tril down
					if(userProcess){
						userId = includeUserId(userProcess,isUserIdInKey,studentIds,userId,columnMetaInfo[0],metricRow,usageMap,studentContentMapper);
					}
					//Since content are stored in column,we need to do a content based separation
					if(idBasedContentUsage.containsKey(columnMetaInfo[0])){
						usageMap.putAll(idBasedContentUsage.get(columnMetaInfo[0]));
					}
					idBasedContentUsage.put(columnMetaInfo[0], usageMap);
					//Storing the Fetched content id to support including default value at content level 
					if(!fetchedContentIds.contains(columnMetaInfo[0]) && columnMetaInfo[0].length() > 35){
						fetchedContentIds.add(columnMetaInfo[0]);
					}
				}
				contentUsageData.addAll(getBaseService().convertMapToList(idBasedContentUsage, ApiConstants.GOORUOID));
			}
		}
		
		/**
		 * Set default value at user-content level
		 */
		if(userProcess){
			insertDefaultUserContents(columnFamily, contentIds, studentContentMapper, studentIds, aliesNames,contentUsageData);
		}else {
			/**
			 * Set default value only at collection level
			 */
			for(String id : contentIds.split(ApiConstants.COMMA)){
				if(!fetchedContentIds.contains(id)){
					Map<String,Object> tempMap = new HashMap<String, Object>();
					DataUtils.fetchDefaultData(columnFamily, aliesNames, tempMap);
					tempMap.put(ApiConstants.GOORUOID, id);
					contentUsageData.add(tempMap);
				}
			}
		}
		return contentUsageData;
	}
	
	private List<Map<String,Object>> getIdSeparatedMetrics(Collection<String> rowKeys,String columnFamily, Collection<String> requestedColumns, String studentIds,boolean isUserIdInKey,String contentIds, boolean userProcess) {

		Collection<String> fetchedContentIds = new ArrayList<String>();
		List<Map<String,Object>> contentUsageData = new ArrayList<Map<String,Object>>();
		Map<String,Set<String>> studentContentMapper = new HashMap<String,Set<String>>();

		/**
		 * Get Activity data
		 */
		OperationResult<Rows<String, String>> activityData = getCassandraService().readAll(columnFamily, rowKeys, requestedColumns);
		if (activityData != null && activityData.getResult() != null) {
			
			Rows<String, String> itemMetricRows = activityData.getResult();
			//Iterate for Every Row
			for(Row<String, String> metricRow : itemMetricRows){
				String userId = null;
				Map<String,Map<String, Object>> idBasedContentUsage = new HashMap<String,Map<String, Object>>();
				//Iterate for Fetched column
				for(String column : requestedColumns){
					Map<String,Object> usageMap = new HashMap<String,Object>();
					String[] columnMetaInfo = column.split(ApiConstants.TILDA);
					String metricName = (columnMetaInfo.length > 1) ? columnMetaInfo[columnMetaInfo.length-1] : columnMetaInfo[0];
					//No Need to process for MA question A,B,C,D option
					if((columnMetaInfo.length > 1 ? columnMetaInfo[1].matches(ApiConstants.OPTIONS_MATCH) : columnMetaInfo[0].matches(ApiConstants.OPTIONS_MATCH))){
						continue;
					}
					//Get the metric data
					usageMap = fetchMetricData(columnMetaInfo[0],metricRow,metricName,column);
					//Get the userId for the content usage,If we need user level tril down
					if(userProcess){
						userId = includeUserId(userProcess,isUserIdInKey,studentIds,userId,columnMetaInfo[0],metricRow,usageMap,studentContentMapper);
					}
					//Since content are stored in column,we need to do a content based separation
					if(idBasedContentUsage.containsKey(columnMetaInfo[0])){
						usageMap.putAll(idBasedContentUsage.get(columnMetaInfo[0]));
					}
					idBasedContentUsage.put(columnMetaInfo[0], usageMap);
					//Storing the Fetched content id to support including default value at content level 
					if(!fetchedContentIds.contains(columnMetaInfo[0]) && columnMetaInfo[0].length() > 35){
						fetchedContentIds.add(columnMetaInfo[0]);
					}
				}
				contentUsageData.addAll(getBaseService().convertMapToList(idBasedContentUsage, ApiConstants.GOORUOID));
			}
		}
		
		/**
		 * Set default value at user-content level
		 */
		if(userProcess){
			insertDefaultUserContents(columnFamily, contentIds, studentContentMapper, studentIds, requestedColumns,contentUsageData);
		}else {
			/**
			 * Set default value only at collection level
			 */
			for(String id : contentIds.split(ApiConstants.COMMA)){
				if(!fetchedContentIds.contains(id)){
					Map<String,Object> tempMap = insertDefaultMetrics(requestedColumns);
					tempMap.put(ApiConstants.GOORUOID, id);
					contentUsageData.add(tempMap);
				}
			}
		}
		return contentUsageData;
	}	
	
	private void getTypeBasedItemGooruOids(String lessonId, StringBuffer itemGooruOids, StringBuffer collectionGooruOids, StringBuffer assessmentGooruOids, StringBuffer assessmentUrlGooruOids) {
		OperationResult<ColumnList<String>> contentItemRows = getCassandraService().read(ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonId);
		if (!contentItemRows.getResult().isEmpty()) {
			
			//fetch item ids w.r.t their resource type
			ColumnList<String> contentItems = contentItemRows.getResult();
			for (Column<String> item : contentItems) {
				String contentType = null;
				String itemGooruOid = item.getName();
				ColumnList<String> resourceData = getCassandraService().read(ColumnFamily.RESOURCE.getColumnFamily(), itemGooruOid).getResult();
				if (!resourceData.isEmpty() && resourceData.size() > 0) {
					contentType = resourceData.getColumnByName(ApiConstants.RESOURCE_TYPE).getStringValue();
				}
				if (contentType != null) {
					if (contentType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
						if (collectionGooruOids.length() > 0) {
							collectionGooruOids.append(COMMA);
						}
						collectionGooruOids.append(itemGooruOid);
					} else if (contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT) || contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT_SLASH_URL)) {
						if (assessmentGooruOids.length() > 0) {
							assessmentGooruOids.append(COMMA);
						}
						assessmentGooruOids.append(itemGooruOid);
					}
				}
				if (itemGooruOids.length() > 0) {
					itemGooruOids.append(COMMA);
				}
				itemGooruOids.append(itemGooruOid);
			}
		}
	}

	public List<Map<String,Object>> getStudents(String classId){

		OperationResult<ColumnList<String>> studentData = getCassandraService().read(ColumnFamily.USER_GROUP_ASSOCIATION.getColumnFamily(), classId);
		List<Map<String,Object>> studentsList = new ArrayList<Map<String,Object>>();
		if(studentData != null) {
			for(Column<String> column : studentData.getResult()){
				Map<String,Object> student = new HashMap<String,Object>();
				student.put(ApiConstants.USER_UID, column.getName());
				student.put(ApiConstants.USER_NAME, column.getStringValue());
				studentsList.add(student);
			}
		}
		return studentsList;
	} 
	
	public List<Map<String, Object>> getAssociatedItems(String classId, String rowKey, String type, boolean fetchMetaData, boolean isSecure, Collection<String> columnNames, Map<String, String> aliasName) {
		OperationResult<ColumnList<String>> associatedItemList = getCassandraService().read(ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), rowKey);
		List<Map<String, Object>> associatedItems = new ArrayList<Map<String, Object>>();
		List<String> itemIds = new ArrayList<String>();
		if (associatedItemList != null && associatedItemList.getResult() != null && associatedItemList.getResult().size() > 0) {

			for (Column<String> column : associatedItemList.getResult()) {
				if (isVisibleCollection(classId,column.getName())) {
					Map<String, Object> itemDataMap = new HashMap<String, Object>();
					itemDataMap.put(ApiConstants.SEQUENCE, column.getLongValue());
					itemDataMap.put(ApiConstants.GOORUOID, column.getName());
					itemIds.add(column.getName());
					associatedItems.add(itemDataMap);
				}
				
			}
			if (fetchMetaData) {
				if(aliasName != null && aliasName.size() > 0){
					columnNames = aliasName.keySet();
				}else if (columnNames == null || columnNames.isEmpty()) {
					columnNames = new ArrayList<String>();
					columnNames.add(ApiConstants.TITLE);
					columnNames.add(ApiConstants.GOORUOID);
					columnNames.add(ApiConstants._GOORUOID);
					columnNames.add(ApiConstants.RESOURCE_TYPE);
				}
				List<Map<String, Object>> itemMetaData = getResourcesMetaData(itemIds, columnNames, type, aliasName,isSecure);
				associatedItems = getBaseService().innerJoin(itemMetaData, associatedItems, ApiConstants.GOORUOID);
			}
		}
		return associatedItems;
	}

	private ResponseParamDTO<Map<String, Object>> getAllStudentProgressByUnit(String classId, String courseId, boolean isSecure) throws Exception {

		List<Map<String, Object>> contentUsage = new ArrayList<Map<String, Object>>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		//validate ClassId
		isValidClass(classId);
		List<Map<String,Object>> unitsMetaData = getAssociatedItems(classId,courseId,null,true, isSecure, null, DataUtils.getResourceFields());

		List<Map<String,Object>> students = getStudents(classId);
		if(!unitsMetaData.isEmpty() && !students.isEmpty()){
		String classCourseId = ServiceUtils.appendTilda(classId,courseId);
		StringBuffer unitIds = ServiceUtils.getCommaSeparatedIds(unitsMetaData, ApiConstants.GOORUOID);
		StringBuffer studentIds = ServiceUtils.getCommaSeparatedIds(students, ApiConstants.USER_UID);
		Collection<String> UnitStudentKeys = ServiceUtils.generateCommaSeparatedStringToKeys(ApiConstants.TILDA, unitIds.toString(),studentIds.toString());
		Collection<String> keys = new ArrayList<String>();
		for(String unitStudentKey : UnitStudentKeys){
			keys.add(ServiceUtils.appendTilda(classCourseId,unitStudentKey));
		}
		contentUsage = getDirectActivityMetrics(keys,ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), studentIds.toString(),true,unitIds.toString(),true, DataUtils.getAllStudentUnitProgress(), isSecure);
		contentUsage = getBaseService().leftJoin(contentUsage,unitsMetaData,ApiConstants.GOORUOID,ApiConstants.GOORUOID);
		//group at content level
		contentUsage = getBaseService().groupRecordsBasedOnKey(contentUsage,ApiConstants.USER_UID,ApiConstants.USAGE_DATA);
		contentUsage = getBaseService().leftJoin(contentUsage, students, ApiConstants.USER_UID, ApiConstants.USER_UID);
		}
		responseParamDTO.setContent(contentUsage);
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> getSessionStatus(String sessionId, String contentGooruId) {

		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		OperationResult<ColumnList<String>> sessionDetails = getCassandraService().read(ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionId);
		if (sessionDetails != null && sessionDetails.getResult() != null) {
			ColumnList<String> sessionList = sessionDetails.getResult();
			Map<String, Object> sessionDataMap = new HashMap<String, Object>();
			String status = sessionList.getStringValue(ServiceUtils.appendTilda(contentGooruId,STATUS), null);
			sessionDataMap.put(ApiConstants.SESSIONID, sessionId);
			if (status != null) {
				status = status.equalsIgnoreCase(ApiConstants.STOP) ? ApiConstants.COMPLETED : ApiConstants.INPROGRESS;
			} else {
				ValidationUtils.rejectInvalidRequest(ErrorCodes.E109, contentGooruId);
			}
			sessionDataMap.put(STATUS, status);
			responseParamDTO.setMessage(sessionDataMap);
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E110, sessionId);
		}
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> findUsageAvailable(String classGooruId, String courseGooruId, String unitGooruId, String lessonGooruId, String contentGooruId) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		//validate ClassId
		isValidClass(classGooruId);
		String key = baseService.appendTilda(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId);
		if (StringUtils.isNotBlank(key)) {
			OperationResult<ColumnList<String>> sessions = getCassandraService().read(ColumnFamily.SESSION.getColumnFamily(), key);
			Map<String, Object> sessionDataMap = new HashMap<String, Object>();
			sessionDataMap.put(USAGE_SIGNALS_AVAILABLE, false);
			if (sessions != null) {
				ColumnList<String> sessionList = sessions.getResult();
				if (!sessionList.isEmpty()) {
					sessionDataMap.put(USAGE_SIGNALS_AVAILABLE, true);
				}
			}
			responseParamDTO.setMessage(sessionDataMap);
		}
		return responseParamDTO;
	}
		
	
	public ResponseParamDTO<Map<String,Object>> getStudentsCollectionData(String classId, String courseId, String unitId, String lessonId, String collectionId, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		//validate ClassId
		isValidClass(classId);
		//Get list of resources and students
		Map<String, String> resourceFields = DataUtils.getResourceFields();
		resourceFields.put(ApiConstants.QUESTION_DOT_TYPE, ApiConstants.QUESTION_TYPE);
		resourceFields.put(ApiConstants.QUESTION_DOT_QUESTION_TYPE, ApiConstants.QUESTION_TYPE);
		List<Map<String,Object>> resourcesMetaData = getAssociatedItems(null,collectionId,null,true,isSecure,resourceFields.keySet(),resourceFields);
		List<Map<String,Object>> studentsMetaData = getStudents(classId);
		
		StringBuffer resourceIds = ServiceUtils.getCommaSeparatedIds(resourcesMetaData, ApiConstants.GOORUOID);
		StringBuffer studentIds = ServiceUtils.getCommaSeparatedIds(studentsMetaData, ApiConstants.USER_UID);
		//Fetch session data
		Collection<String> rowKeys = getBaseService().generateCommaSeparatedStringToKeys(ApiConstants.TILDA, getBaseService().appendTilda(SessionAttributes.RS.getSession(),classId,courseId,unitId,lessonId,collectionId), studentIds.toString());
		List<String> sessionIds = getSessions(rowKeys);
		//Fetch collection activity data
		Collection<String> columns = getBaseService().generateCommaSeparatedStringToKeys(ApiConstants.TILDA, resourceIds.toString(), DataUtils.getStudentsCollectionUsage().keySet());
		columns.add(ApiConstants.GOORU_UID);
		List<Map<String,Object>> assessmentUsage = getIdSeparatedMetrics(sessionIds,ColumnFamily.SESSION_ACTIVITY.getColumnFamily(),columns, DataUtils.getStudentsCollectionUsage(), studentIds.toString(),false,resourceIds.toString(),true);
		assessmentUsage = getBaseService().leftJoin(assessmentUsage, studentsMetaData, ApiConstants.USER_UID, ApiConstants.USER_UID);
		//Group data at user level
		assessmentUsage = getBaseService().groupRecordsBasedOnKey(assessmentUsage,ApiConstants.GOORUOID,ApiConstants.USAGE_DATA);
		assessmentUsage = getBaseService().leftJoin(resourcesMetaData,assessmentUsage,ApiConstants.GOORUOID,ApiConstants.GOORUOID);
		//Setting assessment meta data
        List<Map<String,Object>> assessmentMetaInfo = getQuestionMetaData(collectionId);
        assessmentUsage = getBaseService().leftJoin(assessmentUsage, assessmentMetaInfo, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
		responseParamDTO.setContent(assessmentUsage);
		return responseParamDTO;
	}
	
	public List<String> getSessions(Collection<String> rowKeys) {
		
		List<String> sessions = new ArrayList<String>();
		OperationResult<Rows<String, String>> sessionItems = getCassandraService().read(ColumnFamily.SESSION.getColumnFamily(), rowKeys);
		if(!sessionItems.getResult().isEmpty()){
			for(Row<String,String> sessionRow : sessionItems.getResult()) {
				String sessionId = sessionRow.getColumns().getStringValue(ApiConstants._SESSION_ID, null);
				if(sessionId != null){
					sessions.add(sessionId);
				}
			}
		}
		return sessions;
	}
	
	private List<Map<String,Object>> getQuestionMetaData(String collectonId){
		Collection<String> columns = new ArrayList<String>();
		OperationResult<Rows<String, String>> questionMetaDatas = getCassandraService().readAll(ColumnFamily.ASSESSMENT_ANSWER.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, collectonId, columns);
		Map<String,Object> resultMap = new HashMap<String,Object>();
		for(Row<String, String> row : questionMetaDatas.getResult()){
			Map<String,Object> dataMap = new HashMap<String,Object>();
			String key = row.getColumns().getStringValue(ApiConstants.QUESTION_GOORU_OID, null);
			dataMap.put(ApiConstants._QUESTIONGOORUOID, row.getColumns().getStringValue(ApiConstants.QUESTION_GOORU_OID, null));
			dataMap.put(ApiConstants.QUESTION_TYPE, row.getColumns().getStringValue(ApiConstants._QUESTION_TYPE, null));
			dataMap.put(ApiConstants.SEQUENCE, row.getColumns().getIntegerValue(ApiConstants.SEQUENCE, 0));
			dataMap.put(ApiConstants.IS_CORRECT, row.getColumns().getIntegerValue(ApiConstants._IS_CORRECT, 0));
			dataMap.put(ApiConstants.ANSWER_ID, row.getColumns().getLongValue(ApiConstants._ANSWER_ID, 0L));
			dataMap.put(ApiConstants._ANSWERTEXT, row.getColumns().getStringValue(ApiConstants.ANSWER_TEXT, null));
			dataMap.put(ApiConstants.QUESTION_ID, row.getColumns().getLongValue(ApiConstants._QUESTION_ID, 0L));
			dataMap.put(ApiConstants.TYPE, row.getColumns().getStringValue(ApiConstants._TYPE_NAME, null));
			Map<String,Object> tempMap = new HashMap<String,Object>();
			if(resultMap.containsKey(key)){
				Map<String,Object> storedData = (Map<String, Object>) resultMap.get(key);
				List<Map<String,Object>> dataList = (List<Map<String, Object>>) storedData.get(ApiConstants.META_DATA);
				dataList.add(dataMap);
				tempMap.put(ApiConstants.META_DATA, dataList);
			}else{
				List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
				dataList.add(dataMap);
				tempMap.put(ApiConstants.META_DATA, dataList);
				
			}
			resultMap.put(row.getColumns().getStringValue(ApiConstants.QUESTION_GOORU_OID, null), tempMap);
		}
		return getBaseService().convertMapToList(resultMap, ApiConstants.GOORUOID);
	}

	private Map<String, Object> insertDefaultMetrics(Collection<String> columns) {
		Map<String, Object> usageMap = new HashMap<String, Object>();
		for (String metricName : columns) {
			if (metricName.endsWith(ApiConstants._COLLECTION_TYPE)) {
				usageMap.put(ApiConstants.COLLECTION_TYPE, null);
			} else if (metricName.endsWith(ApiConstants.VIEWS)) {
				usageMap.put(ApiConstants.VIEWS, 0L);
			} else if (metricName.endsWith(ApiConstants._SCORE_IN_PERCENTAGE)) {
				usageMap.put(ApiConstants.SCORE_IN_PERCENTAGE, 0L);
			} else if (metricName.endsWith(ApiConstants._ANSWER_OBJECT)) {
				usageMap.put(ApiConstants.ANSWER_OBJECT, null);
			}else if(metricName.endsWith(ApiConstants.ATTEMPTS)){
				usageMap.put(ApiConstants.ATTEMPTS, 0L);
			}else if(metricName.endsWith(ApiConstants.SCORE)){
				usageMap.put(ApiConstants.SCORE, 0L);
			}else if(metricName.endsWith(ApiConstants._AVG_TIME_SPENT)) {
				usageMap.put(ApiConstants.AVG_TIME_SPENT, 0L);
			}else if(metricName.endsWith(ApiConstants.CHOICE)) {
				usageMap.put(ApiConstants.TEXT, null);
			}else if(metricName.endsWith(ApiConstants._AVG_REACTION)){
				usageMap.put(ApiConstants.AVG_REACTION, 0L);
			}else if(metricName.endsWith(ApiConstants.REACTION)){
				usageMap.put(ApiConstants.REACTION, 0L);
			}else if(metricName.endsWith(ApiConstants._QUESTION_STATUS)){
				usageMap.put(ApiConstants.STATUS, null);
			}else if(metricName.endsWith(ApiConstants._TIME_SPENT)) {
				usageMap.put(ApiConstants.TIMESPENT, 0L);
			} else if(metricName.endsWith(ApiConstants.OPTIONS)) {
				usageMap.put(ApiConstants.OPTIONS, null);
			}
		}
		return usageMap;
	}
	
	private Map<String,Object> fetchMetricData(String id,Row<String, String> metricRow, String metricName,String column){
		Map<String,Object> usageMap = new HashMap<String,Object>();
 		if(metricName.equals(ApiConstants._COLLECTION_TYPE)){
			usageMap.put(ApiConstants.COLLECTION_TYPE, metricRow.getColumns().getStringValue(column, null));
		}else if(metricName.equals(ApiConstants.VIEWS)){
			usageMap.put(ApiConstants.VIEWS, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants._SCORE_IN_PERCENTAGE)){
			usageMap.put(ApiConstants.SCORE_IN_PERCENTAGE, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants._TIME_SPENT)){
			usageMap.put(ApiConstants.TIMESPENT, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if (metricName.equals(ApiConstants._AVG_TIME_SPENT)) {
			usageMap.put(ApiConstants.AVG_TIME_SPENT, metricRow.getColumns().getLongValue(column.trim(), 0L));
		} else if(metricName.equals(ApiConstants._ANSWER_OBJECT)){
			usageMap.put(ApiConstants.ANSWER_OBJECT, metricRow.getColumns().getStringValue(column.trim(), null));
		}else if(metricName.equals(ApiConstants.ATTEMPTS)){
			usageMap.put(ApiConstants.ATTEMPTS, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants.CHOICE)){
			usageMap.put(ApiConstants.TEXT, metricRow.getColumns().getStringValue(column.trim(), null));
		}else if(metricName.equals(ApiConstants.REACTION)){
			usageMap.put(ApiConstants.REACTION, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants._AVG_REACTION)){
			usageMap.put(ApiConstants.AVG_REACTION, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants._QUESTION_STATUS)){
			String responseStatus = metricRow.getColumns().getStringValue(column.trim(), null);
			usageMap.put(ApiConstants.STATUS, responseStatus);
			usageMap.put(ApiConstants.SCORE, (responseStatus != null && responseStatus.equalsIgnoreCase(ApiConstants.CORRECT)) ? 1L : 0L);
		}else if(metricName.matches(ApiConstants.OPTIONS)){
			Map<String,Long> optionMap = new HashMap<String,Long>();
			optionMap.put(ApiConstants.options.A.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.A.name()), 0L));
			optionMap.put(ApiConstants.options.B.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.B.name()), 0L));
			optionMap.put(ApiConstants.options.C.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.C.name()), 0L));
			optionMap.put(ApiConstants.options.D.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.D.name()), 0L));
			optionMap.put(ApiConstants.options.E.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.E.name()), 0L));
			optionMap.put(ApiConstants.options.F.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.F.name()), 0L));
			usageMap.put(ApiConstants.OPTIONS,optionMap);
		}else if(metricName.equalsIgnoreCase(ApiConstants.GOORU_UID)){
			usageMap.put(ApiConstants.USER_UID, metricRow.getColumns().getStringValue(ApiConstants.GOORU_UID, null));
		}else {
			try{
				usageMap.put(metricName, metricRow.getColumns().getStringValue(column, null));
			}catch(Exception e){
				InsightsLogger.error(getBaseService().errorHandler(ErrorMessages.UNHANDLED_EXCEPTION, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), column),e);
			}
		}
 		return usageMap;
	}
	
	private void insertDefaultUserContents(String columnFamily, String collectionIds, Map<String,Set<String>> userSet, String userIds,Map<String,String> aliesNames, List<Map<String,Object>> collectionUsageData){
	
		for(String collectionId : collectionIds.split(ApiConstants.COMMA)){
			Map<String,Object> tempMap = new HashMap<String, Object>();
			DataUtils.fetchDefaultData(columnFamily, aliesNames, tempMap);
			tempMap.put(ApiConstants.GOORUOID, collectionId);
			if(userSet.containsKey((collectionId))){
				insertDataToDefaultUsers(userSet,collectionId,userIds,tempMap,collectionUsageData);
			}else{
				userSet.put(collectionId, new HashSet<String>());
				insertDataToDefaultUsers(userSet,collectionId,userIds,tempMap,collectionUsageData);
			}
		}
	}
	
	private void insertDefaultUserContents(String columnFamily, String collectionIds, Map<String,Set<String>> userSet, String userIds,Collection<String> columns, List<Map<String,Object>> collectionUsageData){
		
		for(String collectionId : collectionIds.split(ApiConstants.COMMA)){
			Map<String,Object> tempMap = insertDefaultMetrics(columns);
			tempMap.put(ApiConstants.GOORUOID, collectionId);
			if(userSet.containsKey((collectionId))){
				insertDataToDefaultUsers(userSet,collectionId,userIds,tempMap,collectionUsageData);
			}else{
				userSet.put(collectionId, new HashSet<String>());
				insertDataToDefaultUsers(userSet,collectionId,userIds,tempMap,collectionUsageData);
			}
		}
	}
	
	private void insertDataToDefaultUsers(Map<String,Set<String>> userSet,String collectionId,String userIds,Map<String,Object> tempMap, List<Map<String,Object>> collectionUsageData){
		Set<String> temp = userSet.get(collectionId);
		for(String user : userIds.split(ApiConstants.COMMA)){
			Map<String,Object> userData = new HashMap<String,Object>(tempMap);
			if(!temp.contains(user)){
				userData.put(ApiConstants.USER_UID, user);
				collectionUsageData.add(userData);
			}
		}
	}
	private String includeUserId(boolean userProcess,boolean isUserIdInKey,String userIds,String userId,String contentId,Row<String,String> metricRow,Map<String,Object> usageMap,Map<String,Set<String>> studentContentMapper){
		
		/**
		 * Assign the User Id, Check is the user exists in key then iterate the userIds
		 *  or else fetch from column
		 */
		if(userId == null){
			if(isUserIdInKey){
				for(String id : userIds.split(ApiConstants.COMMA)){
					if(metricRow.getKey().contains(id)){
						userId = id;
						break;
					}
				}
			}else{
				if(metricRow.getColumns().getColumnNames().contains(ApiConstants.GOORU_UID)){
					userId = metricRow.getColumns().getStringValue(ApiConstants.GOORU_UID, null);
				}
			}
		}
		usageMap.put(ApiConstants.USER_UID, userId);

		/**
		 * Store the User Ids to every content,so that it helps to include default value.
		 */
		if(userProcess){
		Set<String>	setHandler = new HashSet<String>();
		setHandler.add(userId);
			if(studentContentMapper.containsKey(contentId)){
				setHandler.addAll(studentContentMapper.get(contentId));
			}
		studentContentMapper.put(contentId, setHandler);
		}
		return userId;
	}

	public ResponseParamDTO<Map<String, Object>> getStudentAssessmentSummary(String classId, String courseId, String unitId, String lessonId, String assessmentId, String userUid,
			String sessionId, boolean isSecure) throws Exception {

		List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		//validate ClassId
		isValidClass(classId);
		//Fetch sessionId from recent session if sessionId is not requested in call
		String recentSessionKey = null;
		if ((sessionId != null && StringUtils.isNotBlank(sessionId.trim()))) {
			recentSessionKey = sessionId;
		} else if (StringUtils.isNotBlank(classId) && StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(unitId) 
				&&  StringUtils.isNotBlank(lessonId) && StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(assessmentId)) {
			recentSessionKey = getSessionIdFromKey(getBaseService().appendTilda(SessionAttributes.RS.getSession(), classId, courseId, unitId, lessonId, assessmentId, userUid));
		} else if(StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(assessmentId)) {
			recentSessionKey = getSessionIdFromKey(getBaseService().appendTilda(SessionAttributes.RS.getSession(), assessmentId, userUid));
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E112,  
					getBaseService().appendComma("contentGooruId", "sessionId"),
					getBaseService().appendComma("contentGooruId", "sessionId", "classGooruId", "courseGooruId", "unitGooruId", "lessonGooruId"),
					getBaseService().appendComma("contentGooruId", "userUid"));
		}
		
		//Fetch collection summary data
		if (recentSessionKey != null) {
			logger.info("Fetching Collection Summary data for Session Id : " + recentSessionKey);
			itemDataMapAsList = getCollectionSummaryData(assessmentId, recentSessionKey, itemDataMapAsList, isSecure);
		} else {
			logger.info("Recent session is unavailable for collection : " + assessmentId +" and user : "+userUid);
		}
		responseParamDTO.setContent(itemDataMapAsList);
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> getCollectionSummaryData(String collectionGooruId, String sessionId, List<Map<String, Object>> itemDataMapAsList, boolean isSecure) {

		//Fetch collection items
		List<Map<String, Object>> itemColumnResult = getAssociatedItems(null,collectionGooruId, null, false, isSecure, null,null);
		StringBuffer resourceGooruOids = getBaseService().getCommaSeparatedIds(itemColumnResult, ApiConstants.GOORUOID);

		//Resource metadata
		List<Map<String, Object>> rawDataMapAsList = getResourceData(isSecure, resourceGooruOids.toString(), DataUtils.getCollectionSummaryResourceColumns(), ApiConstants.RESOURCE);
		//Usage Data
		Set<String> columnSuffix = DataUtils.getSessionActivityMetricsMap().keySet();
		Collection<String> columns = getBaseService().generateCommaSeparatedStringToKeys(ApiConstants.TILDA, resourceGooruOids.toString(), columnSuffix);
		List<Map<String,Object>> usageDataList = getSessionActivityMetrics(getBaseService().convertStringToCollection(sessionId), ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), columns, resourceGooruOids.toString());
//		List<Map<String,Object>> usageDataList = getCollectionActivityMetrics(getBaseService().convertStringToCollection(sessionId), ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), columns, null, false, itemGooruOids.toString(), false);
		//Question meta 
		List<Map<String,Object>> answerRawData = getQuestionMetaData(collectionGooruId);
		rawDataMapAsList = getBaseService().leftJoin(itemColumnResult, rawDataMapAsList, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
		itemDataMapAsList = getBaseService().leftJoin(rawDataMapAsList, usageDataList, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
		itemDataMapAsList = getBaseService().leftJoin(itemDataMapAsList, answerRawData, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
		
		//Fetch Teacher Detail
		StringBuffer teacherUId = getBaseService().getCommaSeparatedIds(itemDataMapAsList, ApiConstants.FEEDBACKPROVIDER);
		if (teacherUId.length() > 0) {
			String teacherUid = ApiConstants.STRING_EMPTY;
			String[] teacherid = teacherUId.toString().split(COMMA);
			for (String ids : teacherid) {
				if (StringUtils.isNotBlank(ids)) {
					teacherUid = ids;
					break;
				}
			}
			List<Map<String,Object>> teacherData = new ArrayList<Map<String,Object>>();
			Map<String,Object> userMetaInfo = getUserMetaInfo(teacherUid);
			teacherData.add(userMetaInfo);
			itemDataMapAsList = getBaseService().leftJoin(itemDataMapAsList, teacherData, ApiConstants.FEEDBACKPROVIDER, ApiConstants.FEEDBACKPROVIDER);
		}
		return itemDataMapAsList;
	}
	
	private Map<String,Object> getUserMetaInfo(String userId){
		Map<String,Object> userMetaInfo = new HashMap<String,Object>();
		Collection<String> userColumns = new ArrayList<String>();
		userColumns.add(ApiConstants.USERNAME);
		userColumns.add(ApiConstants.GOORUUID);
		OperationResult<ColumnList<String>> userDetails = getCassandraService().read(ColumnFamily.USER.getColumnFamily(), userId,userColumns);
		if(!userDetails.getResult().isEmpty()){
			ColumnList<String> userColumnList = userDetails.getResult();
			userMetaInfo.put(ApiConstants.USER_UID, userColumnList.getStringValue(ApiConstants.GOORUUID, null));
			userMetaInfo.put(ApiConstants.USER_NAME, userColumnList.getStringValue(ApiConstants.USERNAME, null));
		}
		return userMetaInfo;
	}
	
	private List<Map<String, Object>> getSessionActivityMetrics(Collection<String> rowKeys, String columnFamily, Collection<String> columns, String itemGooruOids) {
		List<Map<String, Object>> outputUsageDataAsList = new ArrayList<Map<String, Object>>();
		OperationResult<Rows<String, String>> activityData = getCassandraService().readAll(columnFamily, rowKeys, columns);
		if (!activityData.getResult().isEmpty()) {
			Rows<String, String> itemMetricRows = activityData.getResult();
			for (Row<String, String> metricRow : itemMetricRows) {
				Map<String, Map<String, Object>> KeyUsageAsMap = new HashMap<String, Map<String, Object>>();
				if (!metricRow.getColumns().isEmpty() && metricRow.getColumns().size() > 0) {
					for (String item : itemGooruOids.split(COMMA)) {
						Map<String, Object> usageMap = new HashMap<String, Object>();
						Map<String, Object> optionsAsMap = new HashMap<String, Object>();
						for (String column : columns) {
							if (column.contains(item)) {
								String[] columnMetaInfo = column.split(ApiConstants.TILDA);
								String metricName = (columnMetaInfo.length > 1) ? columnMetaInfo[columnMetaInfo.length - 1] : columnMetaInfo[0];
								if (DataUtils.getStringColumns().containsKey(metricName) && !metricName.matches(ApiConstants.OPTIONS)) {
									usageMap.put(DataUtils.getStringColumns().get(metricName), metricRow.getColumns().getStringValue(column.trim(), null));
								} else if (metricName.matches(ApiConstants.OPTIONS) && StringUtils.isNotBlank(metricRow.getColumns().getStringValue(column.trim(), null)) && !metricRow.getColumns().getStringValue(column.trim(), null).equalsIgnoreCase(ApiConstants.SKIPPED)) {
									optionsAsMap.put(metricRow.getColumns().getStringValue(column.trim(), null), 0);
								} else if (DataUtils.getLongColumns().containsKey(metricName)) {
									usageMap.put(DataUtils.getLongColumns().get(metricName), metricRow.getColumns().getLongValue(column.trim(), 0L));
								}
							}	                                                
						}
						usageMap.put(ApiConstants.OPTIONS, optionsAsMap);
						KeyUsageAsMap.put(item, usageMap);
					}
				}
				outputUsageDataAsList.addAll(getBaseService().convertMapToList(KeyUsageAsMap, ApiConstants.GOORUOID));
			}
		}
		return outputUsageDataAsList.isEmpty() ? null : outputUsageDataAsList;
	}
	
	private Long getClassGoal(String classId) {
		OperationResult<ColumnList<String>>  classMetaData = getCassandraService().read(ColumnFamily.CLASS.getColumnFamily(), classId);
		Long classGoal = 0L;
		if(classMetaData != null && classMetaData.getResult() != null && !classMetaData.getResult().isEmpty()) {
			classGoal = classMetaData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
		}
		return classGoal;
	}
	
	private void getContentMeta(String key, String columnNames, Map<String,Object> dataMap) {
		
		OperationResult<ColumnList<String>> lessonData = getCassandraService().read(ColumnFamily.CONTENT_META.getColumnFamily(), key);
		boolean hasData = (lessonData != null && lessonData.getResult() != null) ? true : false;
		for (String columnName : columnNames.split(ApiConstants.COMMA)) {
			if (hasData) {
				dataMap.put(columnName,
						lessonData.getResult().getLongValue(columnName, 0L));
			} else {
				dataMap.put(columnName, 0L);
			}
		}
	}
	
	private Map<String,Object> generateSessionObject(Column<String> sessionColumn ,ColumnList<String> sessionInfo, boolean openSession){
		Map<String, Object> session = new HashMap<String, Object>();
		session.put(SESSION_ID, sessionColumn.getName());
		session.put(EVENT_TIME, sessionColumn.getLongValue());
		if(openSession) {
			session.put(LAST_ACCESSED_RESOURCE, sessionInfo.getStringValue(ServiceUtils.appendTilda(sessionColumn.getName(), _LAST_ACCESSED_RESOURCE), null));
		}
		return session;
	}
	
	private void isValidClass(String classId) {
		if(StringUtils.isNotBlank(classId)) {
			OperationResult<ColumnList<String>> classQuery = getCassandraService().read(ColumnFamily.CLASS.getColumnFamily(), classId);	
			ColumnList<String> classDetail = null;
			if(classQuery != null && (classDetail = classQuery.getResult()) != null) {
				int status = classDetail.getIntegerValue(ApiConstants.DELETED, 0);
				if(status == 1) {
					throw new NotFoundException("Class Not Found!");
				}
			}
		}
	}

	private boolean isVisibleCollection(String classId, String collectionGooruId) {
		if (StringUtils.isNotBlank(classId)) {
			boolean isVisible = false;
			if (StringUtils.isNotBlank(collectionGooruId)) {
				OperationResult<ColumnList<String>> classQuery = getCassandraService().read(ColumnFamily.CLASS_COLLECTION_SETTINGS.getColumnFamily(),ServiceUtils.appendTilda(classId,collectionGooruId));
				ColumnList<String> visibility = null;
				if (classQuery != null && (visibility = classQuery.getResult()) != null) {
					int status = visibility.getIntegerValue(ApiConstants.VISIBILITY, 0);
					isVisible = status == 1 ? true : false;
				}
			}
			return isVisible;
		} else {
			return true;
		}
	}

	private List<Map<String, Object>> isVisibleCollection(Collection<String> itemIds, String collectionGooruId, ColumnList<String> items) {
		List<Map<String, Object>> associatedItems = new ArrayList<Map<String, Object>>();
		Collection<String> columns = new ArrayList<String>();
		columns.add(ApiConstants.VISIBILITY);
		if (StringUtils.isNotBlank(collectionGooruId)) {
			OperationResult<Rows<String, String>> classQuery = getCassandraService().readAll(ColumnFamily.CLASS_COLLECTION_SETTINGS.getColumnFamily(), itemIds, columns);
			for (Row<String, String> row : classQuery.getResult()) {
				int status = row.getColumns().getIntegerValue(ApiConstants.VISIBILITY, null);
				if (status == 1) {
					Map<String, Object> itemDataMap = new HashMap<String, Object>();
					String collectionId = row.getKey().split(SEPARATOR)[1];
					itemDataMap.put(ApiConstants.SEQUENCE, items.getLongValue(collectionId, 0L));
					itemDataMap.put(ApiConstants.GOORUOID, collectionId);
					associatedItems.add(itemDataMap);
				}
			}
		}
		return associatedItems;
	}

}
