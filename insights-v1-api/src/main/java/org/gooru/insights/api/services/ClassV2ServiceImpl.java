package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.constants.InsightsConstant;
import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.utils.ServiceUtils;
import org.gooru.insights.api.utils.ValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.SetSerializer;

import rx.Observable;
import rx.schedulers.Schedulers;

@Service
public class ClassV2ServiceImpl implements ClassV2Service, InsightsConstant{
	
	private static final Logger logger = LoggerFactory.getLogger(ClassV2ServiceImpl.class);

    private final ExecutorService observableExecutor = Executors.newFixedThreadPool(10);
    
	@Autowired
	private CassandraV2Service cassandraService;
	
	@Autowired
	private LambdaService lambdaService;

	private CassandraV2Service getCassandraService() {
		return cassandraService;
	}
	
	@Autowired
	private BaseService baseService;

	private BaseService getBaseService() {
		return baseService;
	}

	public ResponseParamDTO<Map<String, Object>> getSessionStatus(String contentGooruId, String userUId, String sessionId) {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		CqlResult<String, String> sessionDetails = getCassandraService().readWithCondition(ColumnFamily.USER_SESSIONS.getColumnFamily(), new String[][]{{ApiConstants._COLLECTION_UID,contentGooruId},{ApiConstants._USER_UID,userUId},{ApiConstants._SESSION_ID,sessionId}});
		if (sessionDetails != null && sessionDetails.hasRows()) {
			Rows<String, String> sessionList = sessionDetails.getRows();
			for(Row<String, String> row : sessionList) {
				Map<String, Object> sessionDataMap = new HashMap<String, Object>();
				String status = row.getColumns().getStringValue(ApiConstants._EVENT_TYPE, ApiConstants.STRING_EMPTY);
				sessionDataMap.put(ApiConstants.SESSIONID, sessionId);
				status = status.equalsIgnoreCase(ApiConstants.STOP) ? ApiConstants.COMPLETED : ApiConstants.INPROGRESS;
				sessionDataMap.put(InsightsConstant.STATUS, status);
				responseParamDTO.setMessage(sessionDataMap);
			}
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E110, sessionId);
		}
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> getUserSessions(String classId, String courseId, String unitId,
			String lessonId, String collectionId, String collectionType, String userUid) throws Exception {
		String whereCondition = null;
		// TODO Enabled for class verification
		// isValidClass(classId);
		if (StringUtils.isNotBlank(classId) && StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(unitId)
				&& StringUtils.isNotBlank(lessonId) && StringUtils.isNotBlank(collectionId)
				&& StringUtils.isNotBlank(userUid)) {
			whereCondition = CassandraV2ServiceImpl.appendWhere(
					new String[][] { { ApiConstants._CLASS_UID, classId }, { ApiConstants._COURSE_UID, courseId },
							{ ApiConstants._UNIT_UID, unitId }, { ApiConstants._LESSON_UID, lessonId },
							{ ApiConstants._COLLECTION_UID, collectionId }, { ApiConstants._USER_UID, userUid } });
		} else if (StringUtils.isNotBlank(collectionId) && StringUtils.isNotBlank(userUid)) {
			whereCondition = CassandraV2ServiceImpl.appendWhere(new String[][] {
					{ ApiConstants._COLLECTION_UID, collectionId }, { ApiConstants._USER_UID, userUid } });
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E106);
		}
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> resultSet = getSessionInfo(whereCondition, collectionType);
		resultSet = ServiceUtils.sortBy(resultSet, InsightsConstant.EVENT_TIME, ApiConstants.ASC);
		responseParamDTO.setContent(addSequence(resultSet));
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> getSessionInfo(String whereCondition, String collectionType) {
		
		CqlResult<String, String> sessions = getCassandraService().readWithCondition(ColumnFamily.USER_SESSIONS.getColumnFamily(), whereCondition);
		List<Map<String,Object>> sessionList = new ArrayList<Map<String,Object>>();
		if( sessions != null && sessions.hasRows()) {
			for(Row<String,String> row : sessions.getRows()) {
				ColumnList<String> columnList = row.getColumns();
				boolean include = true;
				if (collectionType.equalsIgnoreCase(InsightsConstant.ASSESSMENT) && !columnList.getStringValue(ApiConstants._EVENT_TYPE, ApiConstants.STRING_EMPTY).equalsIgnoreCase(InsightsConstant.STOP)) {
					include = false;
				}
				if(include) {
					Map<String, Object> sessionMap = new HashMap<String,Object>();
					sessionMap.put(InsightsConstant.SESSION_ID,columnList.getStringValue(ApiConstants._SESSION_ID, null));
					sessionMap.put(InsightsConstant.EVENT_TIME,columnList.getLongValue(ApiConstants._EVENT_TIME, 0L));
					sessionList.add(sessionMap);
				}
			}
		}
		return sessionList;
	}
	
	private List<Map<String, Object>> addSequence(List<Map<String, Object>> resultSet) {
		List<Map<String, Object>> finalSet = null;
		if (resultSet != null) {
			int sequence = 1;
			finalSet = new ArrayList<Map<String, Object>>();
			for (Map<String, Object> resultMap : resultSet) {
				resultMap.put(InsightsConstant.SEQUENCE, sequence++);
				finalSet.add(resultMap);
			}
		}
		return finalSet;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getUserCurrentLocationInLesson(String userUid, String classId) {
		Observable<ResponseParamDTO<Map<String, Object>>> userLocationObservable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getUserCurrentLocation(userUid, classId));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		ResponseParamDTO<Map<String, Object>> responseParamDTO = userLocationObservable.toBlocking().first();
		return responseParamDTO;
	}
	
	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getUserPeers(String classId, String courseId, String unitId, String lessonId, String nextLevelType) {
		Observable<ResponseParamDTO<Map<String, Object>>> observable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getUserPeersDetail(classId, courseId, unitId, lessonId, nextLevelType));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getPerformance(String classId, String courseId, String unitId, String lessonId, String userUid, String collectionType, String nextLevelType) {
		Observable<ResponseParamDTO<Map<String, Object>>> observable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getPerformanceData(classId, courseId, unitId, lessonId, userUid, collectionType, nextLevelType));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getAllStudentPerformance(String classId, String courseId, String unitId, String lessonId, String gooruOid, String collectionType) {
		Observable<ResponseParamDTO<Map<String, Object>>> observable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			try {
				s.onNext(getAllStudentsPerformance(classId, courseId, unitId, lessonId, gooruOid, collectionType));
			} catch (Exception e) {
				logger.error("Exception while fetching all student performance data", e);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	private ResponseParamDTO<Map<String, Object>> getUserCurrentLocation(String userUid, String classId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		ColumnList<String> resultColumns = getCassandraService().getUserCurrentLocation(ColumnFamily.STUDENT_LOCATION.getColumnFamily(), userUid, classId);
		if (resultColumns != null && resultColumns.size() > 0) {
			Map<String, Object> dataAsMap = new HashMap<String, Object>();
			dataAsMap.put("classId", resultColumns.getStringValue(ApiConstants._CLASS_UID, null));
			dataAsMap.put("courseId", resultColumns.getStringValue(ApiConstants._COURSE_UID, null));
			dataAsMap.put("unitId", resultColumns.getStringValue(ApiConstants._UNIT_UID, null));
			dataAsMap.put("lessonId", resultColumns.getStringValue(ApiConstants._LESSON_UID, null));
			dataAsMap.put(ApiConstants.getResponseNameByType(resultColumns.getStringValue(ApiConstants._COLLECTION_TYPE, ApiConstants.CONTENT)), resultColumns.getStringValue(ApiConstants._COLLECTION_UID, null));
			dataMapAsList.add(dataAsMap);
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<Map<String, Object>> getUserPeersDetail(String classId, String courseId, String unitId, String lessonId, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		String rowKey = getBaseService().appendTilda(classId, courseId, unitId, lessonId);
		Rows<String, String> resultRows = getCassandraService().readColumnsWithKey(ColumnFamily.CLASS_ACTIVITY_PEER_DETAIL.getColumnFamily(), rowKey);
		if (resultRows != null && resultRows.size() > 0) {
			for(Row<String, String> resultRow : resultRows) {
				Map<String, Object> dataAsMap = new HashMap<String, Object>(5);
				SetSerializer<String> setSerializer = new SetSerializer<String>(UTF8Type.instance);
				ColumnList<String> columnList = resultRow.getColumns();
				Set<String> activePeers = columnList.getValue(ApiConstants._ACTIVE_PEERS, setSerializer, new HashSet<String>());
				Set<String> leftPeers = columnList.getValue(ApiConstants._LEFT_PEERS, setSerializer, new HashSet<String>());
				dataAsMap.put(ApiConstants.ACTIVE_PEER_COUNT, leftPeers.size());
				dataAsMap.put(ApiConstants.LEFT_PEER_COUNT, activePeers.size());
				nextLevelType = columnList.getStringValue(ApiConstants._COLLECTION_TYPE, ApiConstants.CONTENT);
				if(nextLevelType.matches(ApiConstants.COLLECTION_OR_ASSESSMENT)) {
					dataAsMap.put(ApiConstants.ACTIVE_PEER_UIDS, leftPeers);
					dataAsMap.put(ApiConstants.LEFT_PEER_UIDS, activePeers);
				}
				dataAsMap.put(ApiConstants.getResponseNameByType(nextLevelType), columnList.getStringValue(ApiConstants._LEAF_GOORU_OID, null));

				dataMapAsList.add(dataAsMap);
			}
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> getSummaryData(String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid,
			String collectionType) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> summaryData = new ArrayList<Map<String, Object>>();
		Map<String,Object> usageData = new HashMap<String,Object>();
		List<Map<String,Object>> sessionActivities = new ArrayList<Map<String,Object>>();
		String sessionKey = null;
		//TODO validate ClassId
		//isValidClass(classId);
		if (StringUtils.isNotBlank(sessionId)) {
			sessionKey = sessionId;
		} else if (StringUtils.isNotBlank(classId) && StringUtils.isNotBlank(courseId) 
				&& StringUtils.isNotBlank(unitId) && StringUtils.isNotBlank(lessonId)) {
			ResponseParamDTO<Map<String, Object>> sessionObject = getUserSessions(classId, courseId, unitId,lessonId, assessmentId, collectionType, userUid);
			List<Map<String,Object>> sessionList = sessionObject.getContent();
			sessionKey = sessionList.size() > 0 ? sessionList.get(sessionList.size()-1).get(InsightsConstant.SESSION_ID).toString() : null;
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E111, getBaseService().appendComma("CUL Heirarchy", InsightsConstant.SESSION_ID)
					, getBaseService().appendComma("CUL Heirarchy", InsightsConstant.SESSION_ID));
		}
		
		//Fetch Usage Data
		if (StringUtils.isNotBlank(sessionKey)) {
			getResourceMetricsBySession(sessionActivities, sessionKey, usageData);
		}
		summaryData.add(usageData);
		responseParamDTO.setContent(summaryData);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<Map<String, Object>> getPerformanceData(String classId, String courseId, String unitId, String lessonId, String userUid, String collectionType, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		String rowKey = getBaseService().appendTilda(classId, courseId, unitId, lessonId);
		String whereCondition = null;
		if (StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(collectionType)) {
			whereCondition = CassandraV2ServiceImpl.appendWhere(new String[][] { { ApiConstants._ROW_KEY, rowKey }, { ApiConstants._USER_UID, userUid },{ ApiConstants._COLLECTION_TYPE, collectionType } });
		} else if (StringUtils.isNotBlank(collectionType)) {
			whereCondition = CassandraV2ServiceImpl.appendWhere(new String[][] { { ApiConstants._ROW_KEY, rowKey }, { ApiConstants._COLLECTION_TYPE, collectionType } });
		}
		
		CqlResult<String, String> resultRows = getCassandraService().readWithCondition(ColumnFamily.CLASS_ACTIVITY_DATACUBE.getColumnFamily(), whereCondition);
		if (resultRows.getRows() != null && resultRows.getRows().size() > 0) {
			Map<String, Object> userUsageAsMap = new HashMap<String, Object>();
			for (Row<String, String> resultRow : resultRows.getRows()) {
				List<Map<String, Object>> dataMapList = new ArrayList<Map<String, Object>>();
				String userId = resultRow.getColumns().getStringValue(ApiConstants._USER_UID, null);
				if (userUsageAsMap.containsKey(userId) && userUsageAsMap.get(userId) != null) {
					dataMapList = (List<Map<String, Object>>) userUsageAsMap.get(userId);
				}
				addPerformanceMetrics(dataMapList, resultRow.getColumns(), collectionType, nextLevelType);
				userUsageAsMap.put(userId, dataMapList);
			}
			for (Map.Entry<String, Object> userUsageAsMapEntry : userUsageAsMap.entrySet()) {
				Map<String, Object> resultAsMap = new HashMap<String, Object>(2);
				resultAsMap.put(ApiConstants.USERUID, userUsageAsMapEntry.getKey());
				resultAsMap.put(ApiConstants.USAGE_DATA, userUsageAsMapEntry.getValue());
				dataMapAsList.add(resultAsMap);
			}
			responseParamDTO.setContent(dataMapAsList);
		}
		return responseParamDTO;
	}
	
	//TODO nextLevelType is hard coded temporarily. In future, store and get nextLevelType from CF
	private void addPerformanceMetrics(List<Map<String, Object>> dataMapList, ColumnList<String> columns, String collectionType, String nextLevelType) {
		Map<String, Object> dataAsMap = new HashMap<String, Object>(4);
		String responseNameForViews = ApiConstants.VIEWS;
		if(nextLevelType.equalsIgnoreCase(ApiConstants.CONTENT)) {
			nextLevelType = collectionType;
		}
		if(collectionType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) responseNameForViews = ApiConstants.ATTEMPTS;
		dataAsMap.put(ApiConstants.getResponseNameByType(nextLevelType), columns.getStringValue(ApiConstants._LEAF_NODE, null));
		dataAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, columns.getLongValue(ApiConstants.SCORE, 0L));
		dataAsMap.put(responseNameForViews, columns.getLongValue(ApiConstants.VIEWS, 0L));
		dataAsMap.put(ApiConstants.TIMESPENT, columns.getLongValue(ApiConstants._TIME_SPENT, 0L));
		dataAsMap.put(ApiConstants.COMPLETED_COUNT, columns.getLongValue(ApiConstants.COMPLETED_COUNT, 0L));
		//TODO Need to add logic to fetch total count meta data from Database
		dataAsMap.put(ApiConstants.TOTAL_COUNT, columns.getLongValue(ApiConstants.TOTAL_COUNT, 0L));
		dataMapList.add(dataAsMap);
	}
	
	public ResponseParamDTO<Map<String, Object>> getAllStudentsPerformance(String classId, String courseId, String unitId, String lessonId, String gooruOid, String collectionType) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> resultDataAsList = new ArrayList<Map<String, Object>>(); 
		//TODO store and fetch student list of given class
		List<String> userIds = new ArrayList<String>();
		/*userIds.add("0219090c-abe6-4a09-8c9f-343911f5cd86");
		userIds.add("6f337b1c-0b0d-49b3-8314-e279181aeddf");*/
		
		for (String userId : userIds) {
			Map<String, Object> usageData = new HashMap<String, Object>(2);
			List<Map<String, Object>> sessionActivities = new ArrayList<Map<String, Object>>();
			usageData.put(ApiConstants.USER_UID, userId);
			usageData.put(ApiConstants.USAGE_DATA, sessionActivities);
			String sessionKey = getUserLatestSessionId(classId, courseId, unitId, lessonId, gooruOid, collectionType, userId);
			// Fetch Usage Data
			if (StringUtils.isNotBlank(sessionKey)) {
				getResourceMetricsBySession(sessionActivities, sessionKey, new HashMap<String, Object>(2));
			}
			resultDataAsList.add(usageData);
		}
		responseParamDTO.setContent(resultDataAsList);
		return responseParamDTO;
	}

	private void getResourceMetricsBySession(List<Map<String, Object>> sessionActivities, String sessionKey, Map<String, Object> usageData) {
		CqlResult<String, String> userSessionActivityResult = getCassandraService().readWithCondition(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(),
				new String[][] { { ApiConstants._SESSION_ID, sessionKey } });
		if (userSessionActivityResult != null && userSessionActivityResult.hasRows()) {
			for (Row<String, String> userSessionActivityRow : userSessionActivityResult.getRows()) {
				Map<String, Object> sessionActivityMetrics = new HashMap<String, Object>();
				ColumnList<String> sessionActivityColumns = userSessionActivityRow.getColumns();
				String contentType = sessionActivityColumns.getStringValue(ApiConstants._RESOURCE_TYPE, ApiConstants.STRING_EMPTY);
				sessionActivityMetrics.put(ApiConstants.SESSIONID, sessionActivityColumns.getStringValue(ApiConstants._SESSION_ID, null));
				sessionActivityMetrics.put(ApiConstants.GOORUOID, sessionActivityColumns.getStringValue(ApiConstants._GOORU_OID, null));
				sessionActivityMetrics.put(ApiConstants.RESOURCE_TYPE, contentType);
				sessionActivityMetrics.put(ApiConstants.SCORE, sessionActivityColumns.getLongValue(ApiConstants.SCORE, null));
				sessionActivityMetrics.put(ApiConstants.TIMESPENT, sessionActivityColumns.getLongValue(ApiConstants._TIME_SPENT, null));
				sessionActivityMetrics.put(ApiConstants.VIEWS, sessionActivityColumns.getLongValue(ApiConstants.VIEWS, null));
				if (contentType.matches(ApiConstants.COLLECTION_OR_ASSESSMENT)) {
					if (contentType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
						usageData.put(ApiConstants.COLLECTION, sessionActivityMetrics);
					} else if (contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) {
						usageData.put(ApiConstants.ASSESSMENT, sessionActivityMetrics);
						sessionActivityMetrics.remove(ApiConstants.VIEWS);
						sessionActivityMetrics.put(ApiConstants.ATTEMPTS, sessionActivityColumns.getLongValue(ApiConstants.VIEWS, null));
					}
				} else {
					sessionActivityMetrics.put(ApiConstants.QUESTION_TYPE, sessionActivityColumns.getStringValue(ApiConstants._QUESTION_TYPE, null));
					sessionActivityMetrics.put(ApiConstants.ANSWER_OBJECT, sessionActivityColumns.getStringValue(ApiConstants._ANSWER_OBJECT, null));
					sessionActivityMetrics.put(ApiConstants.ATTEMPTS, sessionActivityColumns.getLongValue(ApiConstants.ATTEMPTS, null));
					sessionActivityMetrics.put(ApiConstants.REACTION, sessionActivityColumns.getLongValue(ApiConstants.REACTION, null));
					sessionActivities.add(sessionActivityMetrics);
				}
			}
			usageData.put(ApiConstants.RESOURCES, sessionActivities);
		}
	}

	private String getUserLatestSessionId(String classId, String courseId, String unitId, String lessonId, String gooruOid, String collectionType, String userId) throws Exception {
		ResponseParamDTO<Map<String, Object>> sessionObject;
		sessionObject = getUserSessions(classId, courseId, unitId, lessonId, gooruOid, collectionType, userId);
		List<Map<String, Object>> sessionList = sessionObject.getContent();
		String sessionKey = sessionList.size() > 0 ? sessionList.get(sessionList.size() - 1).get(InsightsConstant.SESSION_ID).toString() : null;
		return sessionKey;
	}

	public ResponseParamDTO<ContentTaxonomyActivity> getStudentTaxonomyPerformance(String studentId, String subjectId, String courseId, String domainId, String subDomainId, String standardsId, String learningTargetId, Integer depth) {
		
		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		CqlResult<String, String> userSessionActivityResult = getCassandraService().readWithCondition(ColumnFamily.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily(),
				new String[][] { { ApiConstants._USER_UID, studentId }, {ApiConstants._SUBJECT_ID, subjectId}, 
			{ApiConstants._COURSE_ID, courseId}, {ApiConstants._DOMAIN_ID, domainId}, 
			{ApiConstants._SUB_DOMAIN_ID, subDomainId}, {ApiConstants._STANDARDS_ID, standardsId}, {ApiConstants._LEARNING_TARGETS_ID, learningTargetId}} );
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		if(userSessionActivityResult != null && userSessionActivityResult.hasRows()) {
			for(Row<String,String> row : userSessionActivityResult.getRows()) {
				ColumnList<String> taxonomyUsage = row.getColumns();
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				contentTaxonomyActivity.setSubjectId(taxonomyUsage.getStringValue(ApiConstants._SUBJECT_ID, null));
				contentTaxonomyActivity.setCourseId(taxonomyUsage.getStringValue(ApiConstants._COURSE_ID, null));
				contentTaxonomyActivity.setDomainId(taxonomyUsage.getStringValue(ApiConstants._DOMAIN_ID, null));
				contentTaxonomyActivity.setSubDomainId(taxonomyUsage.getStringValue(ApiConstants._SUB_DOMAIN_ID, null));
				contentTaxonomyActivity.setStandardsId(taxonomyUsage.getStringValue(ApiConstants._STANDARDS_ID, null));
				contentTaxonomyActivity.setLearningTargetsId(taxonomyUsage.getStringValue(ApiConstants._LEARNING_TARGETS_ID, null));
				contentTaxonomyActivity.setGooruOid(taxonomyUsage.getStringValue(ApiConstants._GOORU_OID, null));
				contentTaxonomyActivity.setUserUid(taxonomyUsage.getStringValue(ApiConstants._USER_UID, null));
				contentTaxonomyActivity.setResourceFormat(taxonomyUsage.getStringValue(ApiConstants._RESOURCE_FORMAT, null));
				contentTaxonomyActivity.setResourceType(taxonomyUsage.getStringValue(ApiConstants._RESOURCE_TYPE, null));
				contentTaxonomyActivity.setScore(taxonomyUsage.getLongValue(ApiConstants.SCORE, 0L));
				contentTaxonomyActivity.setTimespent(taxonomyUsage.getLongValue(ApiConstants.TIMESPENT, 0L));
				contentTaxonomyActivity.setViews(taxonomyUsage.getLongValue(ApiConstants.VIEWS, 0L));
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
			lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, depth);
		}
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
}
