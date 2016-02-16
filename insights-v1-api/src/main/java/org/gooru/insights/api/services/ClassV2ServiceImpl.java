package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.constants.InsightsConstant;
import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.models.UserContentLocation;
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

	public ResponseParamDTO<Map<String, Object>> getSessionStatus(String sessionId, String contentGooruId) {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		CqlResult<String, String> sessionDetails = getCassandraService().readWithCondition(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(), new String[] {ApiConstants._SESSION_ID, ApiConstants._GOORU_OID}, new String[]{sessionId, contentGooruId}, false);
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
			String lessonId, String collectionId, String collectionType, String userUid, boolean fetchOpenSession) throws Exception {

		// TODO Enabled for class verification
		// isValidClass(classId);
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		String whereCondition = null;
		String parameters[] = null;
		if(fetchOpenSession) {
			whereCondition = CassandraV2ServiceImpl.appendWhere(new String[]{ApiConstants._USER_UID,  ApiConstants._COLLECTION_UID, ApiConstants._COLLECTION_TYPE, ApiConstants._CLASS_UID, ApiConstants._COURSE_UID, ApiConstants._UNIT_UID, ApiConstants._LESSON_UID, ApiConstants._EVENT_TYPE}, false);
			parameters = new String[] {userUid, collectionId, collectionType, classId, courseId, unitId, lessonId, ApiConstants.START};
		} else {
			whereCondition = CassandraV2ServiceImpl.appendWhere(new String[]{ApiConstants._USER_UID,  ApiConstants._COLLECTION_UID, ApiConstants._COLLECTION_TYPE, ApiConstants._CLASS_UID, ApiConstants._COURSE_UID, ApiConstants._UNIT_UID, ApiConstants._LESSON_UID}, false);
			parameters = new String[] {userUid, collectionId, collectionType, classId, courseId, unitId, lessonId};
		}
		List<Map<String, Object>> resultSet = getSessionInfo(whereCondition, collectionType, fetchOpenSession, parameters);
		resultSet = ServiceUtils.sortBy(resultSet, InsightsConstant.EVENT_TIME, ApiConstants.ASC);
		responseParamDTO.setContent(addSequence(resultSet));
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> getSessionInfo(String whereCondition, String collectionType, boolean fetchOpenSession, String[] parameters) {
		
		CqlResult<String, String> sessions = getCassandraService().readWithCondition(ColumnFamily.USER_SESSIONS.getColumnFamily(), whereCondition, parameters);
		List<Map<String,Object>> sessionList = new ArrayList<Map<String,Object>>();
		if( sessions != null && sessions.hasRows()) {
			for(Row<String,String> row : sessions.getRows()) {
				ColumnList<String> columnList = row.getColumns();
				boolean include = true;
				if (!fetchOpenSession  && collectionType.equalsIgnoreCase(InsightsConstant.ASSESSMENT) && !columnList.getStringValue(ApiConstants._EVENT_TYPE, ApiConstants.STRING_EMPTY).equalsIgnoreCase(InsightsConstant.STOP)) {
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
			s.onNext(getStudentCurrentLocation(userUid, classId));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		ResponseParamDTO<Map<String, Object>> responseParamDTO = userLocationObservable.toBlocking().first();
		return responseParamDTO;
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
	public Observable<ResponseParamDTO<Map<String, Object>>> getUserPeers(String classId, String courseId, String unitId, String nextLevelType) {
		Observable<ResponseParamDTO<Map<String, Object>>> observable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getUserPeerData(classId, courseId, unitId, nextLevelType));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getUserPeers(String classId, String courseId, String unitId, String lessonId, String nextLevelType) {
		Observable<ResponseParamDTO<Map<String, Object>>> observable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getUserPeerData(classId, courseId, unitId, lessonId, nextLevelType));
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
	
	@Deprecated
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

	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getPriorDetail(String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid,String collectionType, boolean openSession) {
		Observable<ResponseParamDTO<Map<String, Object>>> observable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
					try {
						s.onNext(getPriorUsage(classId, courseId, unitId, lessonId, assessmentId, sessionId, userUid,
								collectionType, openSession));
					} catch (Throwable t) {
						s.onError(t);
					}
					s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	private ResponseParamDTO<Map<String, Object>> getPriorUsage(String classId, String courseId, String unitId,
			String lessonId, String assessmentId, String sessionId, String userUid, String collectionType, boolean openSession) throws Exception {

		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		if(openSession && ApiConstants.COLLECTION.equalsIgnoreCase(collectionType)) {
			openSession = false;
		}
		String sessionKey = getSession(classId, courseId, unitId, lessonId, assessmentId, sessionId, userUid,
				collectionType, openSession);
		if (StringUtils.isNotBlank(sessionKey)) {
			responseParamDTO.setContent(getPriorUsage(sessionKey));
		}
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String, Object>> getSummaryData(String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid,
			String collectionType) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> summaryData = new ArrayList<Map<String, Object>>();
		Map<String,Object> usageData = new HashMap<String,Object>();
		List<Map<String,Object>> sessionActivities = new ArrayList<Map<String,Object>>();
		//TODO validate ClassId
		//isValidClass(classId);
		String sessionKey = getSession(classId, courseId, unitId, lessonId, assessmentId, sessionId, userUid, collectionType, false);
		
		//Fetch Usage Data
		if (StringUtils.isNotBlank(sessionKey)) {
			getResourceMetricsBySession(sessionActivities, sessionKey, usageData);
			summaryData.add(usageData);
		}
		responseParamDTO.setContent(summaryData);
		return responseParamDTO;
	}
	
	private String getSession(String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid, String collectionType, boolean openSession) throws Exception {
		if (StringUtils.isNotBlank(sessionId)) {
			return sessionId;
		} else if (StringUtils.isNotBlank(classId) && StringUtils.isNotBlank(courseId) 
				&& StringUtils.isNotBlank(unitId) && StringUtils.isNotBlank(lessonId)) {
			ResponseParamDTO<Map<String, Object>> sessionObject = getUserSessions(classId, courseId, unitId,lessonId, assessmentId, collectionType, userUid, openSession);
			List<Map<String,Object>> sessionList = sessionObject.getContent();
			return  sessionList.size() > 0 ? sessionList.get(sessionList.size()-1).get(InsightsConstant.SESSION_ID).toString() : null;
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E111, getBaseService().appendComma("CUL Heirarchy", InsightsConstant.SESSION_ID)
					, getBaseService().appendComma("CUL Heirarchy", InsightsConstant.SESSION_ID));
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
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
			dataAsMap.put(ApiConstants.REACTION, columns.getLongValue(ApiConstants.REACTION, 0L));
		}
		if(collectionType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) responseNameForViews = ApiConstants.ATTEMPTS;
		dataAsMap.put(ApiConstants.getResponseNameByType(nextLevelType), columns.getStringValue(ApiConstants._LEAF_NODE, null));
		dataAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, columns.getLongValue(ApiConstants.SCORE, 0L));
		dataAsMap.put(responseNameForViews, columns.getLongValue(ApiConstants.VIEWS, 0L));
		dataAsMap.put(ApiConstants.TIMESPENT, columns.getLongValue(ApiConstants._TIME_SPENT, 0L));
		dataAsMap.put(ApiConstants.COMPLETED_COUNT, columns.getLongValue(ApiConstants._COMPLETED_COUNT, 0L));
		//TODO Need to add logic to fetch total count meta data from Database
		dataAsMap.put(ApiConstants.TOTAL_COUNT, columns.getLongValue(ApiConstants._TOTAL_COUNT, 0L));
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

	private List<Map<String, Object>> getPriorUsage(String sessionKey) {
	
		List<Map<String, Object>> sessionActivities = new ArrayList<Map<String,Object>>();
		CqlResult<String, String> userSessionActivityResult = getCassandraService().readWithCondition(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(), new String[]{ApiConstants._SESSION_ID}, new String[]{sessionKey}, false);
		if (userSessionActivityResult != null && userSessionActivityResult.hasRows()) {
			for (Row<String, String> userSessionActivityRow : userSessionActivityResult.getRows()) {
				Map<String, Object> sessionActivityMetrics = new HashMap<String, Object>();
				ColumnList<String> sessionActivityColumns = userSessionActivityRow.getColumns();
				String contentType = sessionActivityColumns.getStringValue(ApiConstants._RESOURCE_TYPE, ApiConstants.STRING_EMPTY);
				if(contentType.matches(ApiConstants.COLLECTION_OR_ASSESSMENT)) {
					continue;
				} else {
					sessionActivityMetrics.put(ApiConstants.GOORUOID, sessionActivityColumns.getStringValue(ApiConstants._GOORU_OID, null));
					sessionActivityMetrics.put(ApiConstants.ANSWER_OBJECT, ServiceUtils.castJSONToMap(sessionActivityColumns.getStringValue(ApiConstants._ANSWER_OBJECT, null)));
					sessionActivityMetrics.put(ApiConstants.VIEWS, sessionActivityColumns.getLongValue(ApiConstants.VIEWS, 0L));
					sessionActivityMetrics.put(ApiConstants.REACTION, sessionActivityColumns.getLongValue(ApiConstants.REACTION, 0L));
					sessionActivities.add(sessionActivityMetrics);
				}
			}
		}
		return sessionActivities;
	}
	private void getResourceMetricsBySession(List<Map<String, Object>> sessionActivities, String sessionKey, Map<String, Object> usageData) {
		
		CqlResult<String, String> userSessionActivityResult = getCassandraService().readWithCondition(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(), new String[]{ApiConstants._SESSION_ID}, new String[]{sessionKey}, false);
		if (userSessionActivityResult != null && userSessionActivityResult.hasRows()) {
			String itemName = ApiConstants.RESOURCES;
			for (Row<String, String> userSessionActivityRow : userSessionActivityResult.getRows()) {
				Map<String, Object> sessionActivityMetrics = new HashMap<String, Object>();
				ColumnList<String> sessionActivityColumns = userSessionActivityRow.getColumns();
				String contentType = sessionActivityColumns.getStringValue(ApiConstants._RESOURCE_TYPE, ApiConstants.STRING_EMPTY);
				sessionActivityMetrics.put(ApiConstants.SESSIONID, sessionActivityColumns.getStringValue(ApiConstants._SESSION_ID, null));
				sessionActivityMetrics.put(ApiConstants.GOORUOID, sessionActivityColumns.getStringValue(ApiConstants._GOORU_OID, null));
				sessionActivityMetrics.put(ApiConstants.RESOURCE_TYPE, contentType);
				sessionActivityMetrics.put(ApiConstants.SCORE, sessionActivityColumns.getLongValue(ApiConstants.SCORE, 0L));
				sessionActivityMetrics.put(ApiConstants.TIMESPENT, sessionActivityColumns.getLongValue(ApiConstants._TIME_SPENT, 0L));
				sessionActivityMetrics.put(ApiConstants.VIEWS, sessionActivityColumns.getLongValue(ApiConstants.VIEWS, 0L));
				sessionActivityMetrics.put(ApiConstants.REACTION, sessionActivityColumns.getLongValue(ApiConstants.REACTION, 0L));
				if (contentType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
					usageData.put(ApiConstants.COLLECTION, sessionActivityMetrics);
				} else if (contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) {
					usageData.put(ApiConstants.ASSESSMENT, sessionActivityMetrics);
					sessionActivityMetrics.remove(ApiConstants.VIEWS);
					itemName = ApiConstants.QUESTIONS;
					sessionActivityMetrics.put(ApiConstants.ATTEMPTS, sessionActivityColumns.getLongValue(ApiConstants.VIEWS, 0L));
				} else {
					sessionActivityMetrics.put(ApiConstants.QUESTION_TYPE, sessionActivityColumns.getStringValue(ApiConstants._QUESTION_TYPE, null));
					sessionActivityMetrics.put(ApiConstants.ANSWER_OBJECT, ServiceUtils.castJSONToMap(sessionActivityColumns.getStringValue(ApiConstants._ANSWER_OBJECT, null)));
					sessionActivities.add(sessionActivityMetrics);
				}
			}
			usageData.put(itemName, sessionActivities);
		}
	}

	private String getUserLatestSessionId(String classId, String courseId, String unitId, String lessonId, String gooruOid, String collectionType, String userId) throws Exception {
		ResponseParamDTO<Map<String, Object>> sessionObject;
		sessionObject = getUserSessions(classId, courseId, unitId, lessonId, gooruOid, collectionType, userId, false);
		List<Map<String, Object>> sessionList = sessionObject.getContent();
		String sessionKey = sessionList.size() > 0 ? sessionList.get(sessionList.size() - 1).get(InsightsConstant.SESSION_ID).toString() : null;
		return sessionKey;
	}

	public Observable<ResponseParamDTO<ContentTaxonomyActivity>> getUserStandardsMastery(String studentId, String subjectId, String courseId, String domainId, String standardsId, String learningTargetId, Integer depth) {
		
		Observable<ResponseParamDTO<ContentTaxonomyActivity>> observable = Observable.<ResponseParamDTO<ContentTaxonomyActivity>> create(s -> {
			try {
				s.onNext(getTaxonomyActivity(studentId, subjectId, courseId, domainId, standardsId, learningTargetId, depth));
			} catch (Throwable t) {
				s.onError(t);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	public Observable<ResponseParamDTO<ContentTaxonomyActivity>> getUserDomainParentMastery(String studentId, String subjectId, String courseIds, String domainId) {
	
		Observable<ResponseParamDTO<ContentTaxonomyActivity>> observable = Observable.<ResponseParamDTO<ContentTaxonomyActivity>> create(s -> {
			try {
					s.onNext(getUserDomainParentMasteryUsage(studentId, subjectId, courseIds, domainId));		
			} catch (Throwable t) {
				s.onError(t);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getUserDomainParentMasteryUsage(String studentId, String subjectId, String courseIds, String domainId) {
		
		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		List<ContentTaxonomyActivity> activityList = new ArrayList<ContentTaxonomyActivity>();
		if(StringUtils.isEmpty(courseIds)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E104, ApiConstants.COURSE_IDS);
			
		}
		for(String courseId : courseIds.split(ApiConstants.COMMA)) {
			activityList.addAll(getTaxonomyActivity(studentId, subjectId, courseId, domainId, null, null, 1).getContent());
		}
		responseParamDTO.setContent(activityList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getTaxonomyActivity(String studentId, String subjectId, String courseId, String domainId, String standardsId, String learningTargetId, Integer depth) {
		
		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		CqlResult<String, String> userSessionActivityResult = getCassandraService().readWithCondition(ColumnFamily.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily(), 
				new String[]{ApiConstants._USER_UID, ApiConstants._SUBJECT_ID, ApiConstants._COURSE_ID, ApiConstants._DOMAIN_ID, ApiConstants._STANDARDS_ID, ApiConstants._LEARNING_TARGETS_ID}, 
				new String[]{studentId, subjectId, courseId, domainId, standardsId, learningTargetId}, false);
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		Map<String,Set<String>> itemMap = new HashMap<String,Set<String>>();
		if(userSessionActivityResult != null && userSessionActivityResult.hasRows()) {
			for(Row<String,String> row : userSessionActivityResult.getRows()) {
				ContentTaxonomyActivity contentTaxonomyActivity = includeItem(itemMap, row.getColumns(), depth);;
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
			contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, depth);
			combineTaxonomyData(contentTaxonomyActivityList, itemMap, depth);
		}
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> fetchTeacherGrade(String teacherUid, String userUid, String sessionId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		CqlResult<String, String> result = getCassandraService().readWithCondition(ColumnFamily.STUDENT_QUESTION_GRADE.getColumnFamily(), 
				new String[]{ApiConstants._TEACHER_UID, ApiConstants._USER_UID, ApiConstants._SESSION_ID}, 
				new String[]{teacherUid, userUid}, false);
		List<Map<String, Object>> teacherGradeAsList = new ArrayList<>();
		if(result != null && result.hasRows()) {
			for(Row<String,String> row : result.getRows()) {
				Map<String, Object> teacherGradeAsMap = new HashMap<String, Object>();
				ColumnList<String> rowColumnList = row.getColumns();
				teacherGradeAsMap.put(ApiConstants.QUESTION_ID, rowColumnList.getStringValue(ApiConstants._QUESTION_ID, null));
				teacherGradeAsMap.put(ApiConstants.SCORE, rowColumnList.getLongValue(ApiConstants.SCORE, 0L));
				teacherGradeAsList.add(teacherGradeAsMap);
			}
			responseParamDTO.setContent(teacherGradeAsList);
		}
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> getResourceUsage(String sessionId, String resourceIds) {
		
		if(StringUtils.isBlank(resourceIds)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E104, ApiConstants.RESOURCE_IDS);	
		}
		ResponseParamDTO<Map<String, Object>> resourceUsageObject = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String,Object>> resourceUsageList = new ArrayList<Map<String,Object>>();
		for(String resourceId : resourceIds.split(ApiConstants.COMMA)) {
			CqlResult<String, String> userSessionActivityResult = getCassandraService().readWithCondition(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(), new String[]{ApiConstants._SESSION_ID, ApiConstants._GOORU_OID}, new String[]{sessionId, resourceId}, false); 
			if(userSessionActivityResult != null && userSessionActivityResult.hasRows()) {
				Rows<String, String> rows = userSessionActivityResult.getRows();
				for(Row<String,String> row : rows) {
					Map<String, Object> resourceUsage = new HashMap<String,Object>();
					ColumnList<String> userSessionColumn = row.getColumns();
					resourceUsage.put(ApiConstants.GOORUOID, userSessionColumn.getStringValue(ApiConstants._GOORU_OID, ApiConstants.STRING_EMPTY));
					resourceUsage.put(ApiConstants.RESOURCE_TYPE, userSessionColumn.getStringValue(ApiConstants._RESOURCE_TYPE, ApiConstants.STRING_EMPTY));
					resourceUsage.put(ApiConstants.QUESTION_TYPE, userSessionColumn.getStringValue(ApiConstants._QUESTION_TYPE, ApiConstants.STRING_EMPTY));
					resourceUsage.put(ApiConstants.ANSWER_OBJECT, userSessionColumn.getStringValue(ApiConstants._ANSWER_OBJECT, ApiConstants.STRING_EMPTY));
					resourceUsage.put(ApiConstants.STATUS, userSessionColumn.getStringValue(ApiConstants.ANSWER_STATUS, ApiConstants.STRING_EMPTY));
					resourceUsage.put(ApiConstants.VIEWS, userSessionColumn.getLongValue(ApiConstants.VIEWS, 0L));
					resourceUsage.put(ApiConstants.TIMESPENT, userSessionColumn.getLongValue(ApiConstants._TIME_SPENT, 0L));
					resourceUsage.put(ApiConstants.SCORE, userSessionColumn.getLongValue(ApiConstants.SCORE, 0L));
					resourceUsage.put(ApiConstants.ATTEMPTS, userSessionColumn.getLongValue(ApiConstants.ATTEMPTS, 0L));
					resourceUsage.put(ApiConstants.REACTION, userSessionColumn.getLongValue(ApiConstants.REACTION, 0L));
					if(!resourceUsage.isEmpty()) {
						resourceUsageList.add(resourceUsage);		
					}
				}
			}
		}
		resourceUsageObject.setContent(resourceUsageList);	
		return resourceUsageObject;
	}
	
	private ResponseParamDTO<Map<String, Object>> getStudentCurrentLocation(String userUid, String classId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		ColumnList<String> resultColumns = getCassandraService().getUserCurrentLocation(ColumnFamily.STUDENT_LOCATION.getColumnFamily(), userUid, classId);
		if (resultColumns != null && resultColumns.size() > 0) {
			Map<String, Object> dataAsMap = new HashMap<String, Object>();
			dataAsMap.put(ApiConstants.getResponseNameByType(ApiConstants.CLASS), resultColumns.getStringValue(ApiConstants._CLASS_UID, null));
			dataAsMap.put(ApiConstants.getResponseNameByType(ApiConstants.COURSE), resultColumns.getStringValue(ApiConstants._COURSE_UID, null));
			dataAsMap.put(ApiConstants.getResponseNameByType(ApiConstants.UNIT), resultColumns.getStringValue(ApiConstants._UNIT_UID, null));
			dataAsMap.put(ApiConstants.getResponseNameByType(ApiConstants.LESSON), resultColumns.getStringValue(ApiConstants._LESSON_UID, null));
			dataAsMap.put(ApiConstants.getResponseNameByType(resultColumns.getStringValue(ApiConstants._COLLECTION_TYPE, ApiConstants.CONTENT)), resultColumns.getStringValue(ApiConstants._COLLECTION_UID, null));
			dataMapAsList.add(dataAsMap);
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<Map<String, Object>> getUserPeerData(String classId, String courseId, String unitId, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		List<UserContentLocation> userContentLocationObject = new ArrayList<>();
		CqlResult<String, String> resultRows = getCassandraService().getAllUserLocationInClass(ColumnFamily.STUDENT_LOCATION.getColumnFamily(), classId);
		if (resultRows != null && resultRows.getRows() != null && resultRows.getRows().size() > 0) {
			Rows<String, String> rows = resultRows.getRows();
			for (Row<String, String> row : rows) {
				ColumnList<String> columnList = row.getColumns();
				String courseUId = columnList.getStringValue(ApiConstants._COURSE_UID, null);
				String unitUId = columnList.getStringValue(ApiConstants._UNIT_UID, null);
				String lessonUId = columnList.getStringValue(ApiConstants._LESSON_UID, null);
				if (courseUId.equalsIgnoreCase(courseId) && (StringUtils.isNotBlank(unitId) && unitUId.equalsIgnoreCase(unitId)) && StringUtils.isNotBlank(lessonUId)) {
					generateUserLocationObject(classId, nextLevelType, userContentLocationObject, columnList, courseUId, unitUId, lessonUId);
				} else if (courseUId.equalsIgnoreCase(courseId) && StringUtils.isBlank(unitId) && StringUtils.isNotBlank(unitUId)) {
					generateUserLocationObject(classId, nextLevelType, userContentLocationObject, columnList, courseUId, unitUId, lessonUId);
				}
			}
			Map<String, Long> peers = userContentLocationObject.stream().collect(Collectors.groupingBy(object -> {return getGroupByField(object, nextLevelType);}, Collectors.counting()));
			for (Map.Entry<String, Long> peer : peers.entrySet()) {
				Map<String, Object> dataAsMap = new HashMap<String, Object>();
				dataAsMap.put(ApiConstants.getResponseNameByType(nextLevelType), peer.getKey());
				dataAsMap.put(ApiConstants.PEER_COUNT, peer.getValue());
				dataMapAsList.add(dataAsMap);
			}
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}

	private ResponseParamDTO<Map<String, Object>> getUserPeerData(String classId, String courseId, String unitId, String lessonId, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		CqlResult<String, String> resultRows = getCassandraService().getAllUserLocationInClass(ColumnFamily.STUDENT_LOCATION.getColumnFamily(), classId);
		if (resultRows != null && resultRows.getRows() != null && resultRows.getRows().size() > 0) {
			for (Row<String, String> row : resultRows.getRows()) {
				ColumnList<String> columnList = row.getColumns();
				String courseUId = columnList.getStringValue(ApiConstants._COURSE_UID, null); String unitUId = columnList.getStringValue(ApiConstants._UNIT_UID, null);
				String lessonUId = columnList.getStringValue(ApiConstants._LESSON_UID, null); String collectionUId = columnList.getStringValue(ApiConstants._COLLECTION_UID, null);
				nextLevelType = columnList.getStringValue(ApiConstants._COLLECTION_TYPE, ApiConstants.CONTENT);
				if (courseUId.equalsIgnoreCase(courseId) && unitUId.equalsIgnoreCase(unitId) && lessonUId.equalsIgnoreCase(lessonId) && StringUtils.isNotBlank(collectionUId)) {
					String status = ApiConstants.IN_ACTIVE; String userId = columnList.getStringValue(ApiConstants._USER_UID, null); Long sessionTime = columnList.getLongValue(ApiConstants._SESSION_TIME, 0L);
					//TODO make this time limit configurable
					if (sessionTime >= (System.currentTimeMillis() - 900000)) {
						status = ApiConstants.ACTIVE;
					}
					Map<String, Object> dataAsMap = new HashMap<String, Object>(3);
					dataAsMap.put(ApiConstants.USER_UID, userId);
					dataAsMap.put(ApiConstants.STATUS, status);
					dataAsMap.put(ApiConstants.getResponseNameByType(nextLevelType), collectionUId);
					dataMapAsList.add(dataAsMap);
				}
			}
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}
	
	private void generateUserLocationObject(String classId, String nextLevelType, List<UserContentLocation> userContentLocationObject, ColumnList<String> columnList, String courseUId, String unitUId,
			String lessonUId) {
		String userId = columnList.getStringValue(ApiConstants._USER_UID, null);
		UserContentLocation userLocation = new UserContentLocation(classId, courseUId, unitUId, lessonUId, null, userId, nextLevelType);
		userContentLocationObject.add(userLocation);
	}
	
	private String getGroupByField(UserContentLocation userLocation, String nextLevelType) {
		if(nextLevelType.equalsIgnoreCase(ApiConstants.UNIT)) { 
			return userLocation.getUnitUid();
		} else if (nextLevelType.equalsIgnoreCase(ApiConstants.LESSON))  {
			return userLocation.getLessonUid();
		}
		return userLocation.getUnitUid();
	}
	
	private ContentTaxonomyActivity includeItem(Map<String,Set<String>> itemMap, ColumnList<String> taxonomyUsage, Integer depth) {
		
		String parentKey = null;
		String childKey = null;
		ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
		if(depth == 0) {
			parentKey = taxonomyUsage.getStringValue(ApiConstants._SUBJECT_ID, null);
			childKey = taxonomyUsage.getStringValue(ApiConstants._COURSE_ID, null);
			contentTaxonomyActivity.setSubjectId(parentKey);
		} else if (depth == 1) {
			parentKey = taxonomyUsage.getStringValue(ApiConstants._COURSE_ID, null);
			childKey = taxonomyUsage.getStringValue(ApiConstants._DOMAIN_ID, null);
			contentTaxonomyActivity.setCourseId(parentKey);
		} else if (depth == 2) {
			parentKey = taxonomyUsage.getStringValue(ApiConstants._DOMAIN_ID, null);
			childKey = taxonomyUsage.getStringValue(ApiConstants._STANDARDS_ID, null);
			contentTaxonomyActivity.setDomainId(parentKey);
		} else if (depth == 3) {
			parentKey = taxonomyUsage.getStringValue(ApiConstants._STANDARDS_ID, null);
			childKey = taxonomyUsage.getStringValue(ApiConstants._LEARNING_TARGETS_ID, null);
			contentTaxonomyActivity.setStandardsId(parentKey);
		} else if (depth == 4) {
			contentTaxonomyActivity.setLearningTargetsId(taxonomyUsage.getStringValue(ApiConstants._LEARNING_TARGETS_ID, null));
		}
		contentTaxonomyActivity.setResourceType(taxonomyUsage.getStringValue(ApiConstants._RESOURCE_TYPE, null));
		if(ApiConstants.QUESTION.equalsIgnoreCase(contentTaxonomyActivity.getResourceType())) {
			
			contentTaxonomyActivity.setScore(taxonomyUsage.getLongValue(ApiConstants.SCORE, 0L));
			contentTaxonomyActivity.setAttempts(taxonomyUsage.getLongValue(ApiConstants.VIEWS, 0L));
		} else {
			contentTaxonomyActivity.setTimespent(taxonomyUsage.getLongValue(ApiConstants.TIME_SPENT, 0L));
		}
		if(itemMap.containsKey(parentKey)) {
			itemMap.get(parentKey).add(childKey);
		} else {
			Set<String> childItems = new HashSet<String>();
			childItems.add(childKey);
			itemMap.put(parentKey, childItems);
		}
		return contentTaxonomyActivity;
	}
	
	private void combineTaxonomyData(List<ContentTaxonomyActivity> taxonomyActivities, Map<String,Set<String>> itemMap, Integer depth) {
		
		for(ContentTaxonomyActivity contentTaxonomyActivity : taxonomyActivities) {
			
			if(depth == 0) {
				contentTaxonomyActivity.setItemCount(includeItems(contentTaxonomyActivity.getSubjectId(), itemMap));
			} else if(depth == 1) {
				contentTaxonomyActivity.setItemCount(includeItems(contentTaxonomyActivity.getCourseId(), itemMap));
			} else if(depth == 2) {
				contentTaxonomyActivity.setItemCount(includeItems(contentTaxonomyActivity.getDomainId(), itemMap));
			} else if(depth == 3) {
				contentTaxonomyActivity.setItemCount(includeItems(contentTaxonomyActivity.getStandardsId(), itemMap));
			} else if(depth == 4) {
				contentTaxonomyActivity.setItemCount(includeItems(contentTaxonomyActivity.getLearningTargetsId(), itemMap));
			}
		}
	}
	
	private int includeItems(String id, Map<String,Set<String>> itemMap) {
		if(itemMap.containsKey(id)) {
			return itemMap.get(id).size();
		} else {
			return 0;
		}
	}
}
