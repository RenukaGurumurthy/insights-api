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
import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.CqlQueries;
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.constants.InsightsConstant;
import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.models.UserContentLocation;
import org.gooru.insights.api.utils.ServiceUtils;
import org.gooru.insights.api.utils.ValidationUtils;
import org.json.JSONArray;
import org.json.JSONObject;
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

	@Autowired
	private BaseService baseService;

	private BaseService getBaseService() {
		return baseService;
	}
	
	private CassandraV2Service getCassandraService() {
		return cassandraService;
	}

	public ResponseParamDTO<Map<String, Object>> getSessionStatus(String sessionId, String contentGooruId) {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		CqlResult<String, String> sessionDetails = getCassandraService().readRows(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(), CqlQueries.GET_SESSION_ACTIVITY, sessionId, contentGooruId);
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
		List<Map<String, Object>> resultSet =  null;
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
			if(collectionType.equalsIgnoreCase(InsightsConstant.ASSESSMENT)) {
				if(fetchOpenSession) {
					resultSet = getSessionInfo(CqlQueries.GET_USER_ASSESSMENT_SESSIONS, userUid, collectionId, collectionType, classId, courseId, unitId, lessonId, ApiConstants.START);
				} else {
					resultSet = getSessionInfo(CqlQueries.GET_USER_ASSESSMENT_SESSIONS, userUid, collectionId, collectionType, classId, courseId, unitId, lessonId, ApiConstants.STOP);
				}
			} else {
				resultSet = getSessionInfo(CqlQueries.GET_USER_COLLECTION_SESSIONS, userUid, collectionId, collectionType, classId, courseId, unitId, lessonId);
			}
		resultSet = ServiceUtils.sortBy(resultSet, InsightsConstant.EVENT_TIME, ApiConstants.ASC);
		responseParamDTO.setContent(addSequence(resultSet));
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> getSessionInfo(String query, String... parameters) {
		
		CqlResult<String, String> sessions = getCassandraService().readRows(ColumnFamily.USER_SESSIONS.getColumnFamily(), query, parameters);
		List<Map<String,Object>> sessionList = new ArrayList<Map<String,Object>>();
		if( sessions != null && sessions.hasRows()) {
			for(Row<String,String> row : sessions.getRows()) {
				ColumnList<String> columnList = row.getColumns();
					Map<String, Object> sessionMap = new HashMap<String,Object>();
					sessionMap.put(InsightsConstant.SESSION_ID,columnList.getStringValue(ApiConstants._SESSION_ID, null));
					sessionMap.put(InsightsConstant.EVENT_TIME,columnList.getLongValue(ApiConstants._EVENT_TIME, 0L));
					sessionList.add(sessionMap);
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
	public Observable<ResponseParamDTO<Map<String, Object>>> getAllStudentPerformance(String classId, String courseId, String unitId, String lessonId, String gooruOid, String collectionType, String userUid) {
		Observable<ResponseParamDTO<Map<String, Object>>> observable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			try {
				s.onNext(getAllStudentsPerformance(classId, courseId, unitId, lessonId, gooruOid, collectionType, userUid));
			} catch (Exception e) {
				s.onError(e);
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
		CqlResult<String, String> result = getCassandraService().readRows(ColumnFamily.CLASS_ACTIVITY_PEER_DETAIL.getColumnFamily(), CqlQueries.GET_USER_PEER_DETAIL, rowKey);
		Rows<String,String> resultRows = result != null ? result.getRows() : null; 
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
	
	public Observable<ResponseParamDTO<Map<String, Object>>> getSummaryData(String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid,
			String collectionType) throws Exception {
		
		Observable<ResponseParamDTO<Map<String, Object>>> observable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			try {
				s.onNext(fetchSummaryData(classId, courseId, unitId, lessonId, assessmentId, sessionId, userUid,
						collectionType));
			} catch (Throwable t) {
				s.onError(t);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	private ResponseParamDTO<Map<String, Object>> fetchSummaryData(String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid,
			String collectionType) throws Exception {
		
		//TODO validate ClassId
		//isValidClass(classId);
		Map<String,Object> usageData = new HashMap<String,Object>();
		List<Map<String,Object>> sessionActivities = new ArrayList<Map<String,Object>>();
		List<Map<String, Object>> summaryData = new ArrayList<Map<String, Object>>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		String sessionKey = getSession(classId, courseId, unitId, lessonId, assessmentId, sessionId, userUid, collectionType, false);
		if (StringUtils.isNotBlank(sessionKey)) {
			//Fetch Usage Data
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
		CqlResult<String, String> resultRows = null;
		if (StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(collectionType)) {
			resultRows = getCassandraService().readRows(ColumnFamily.CLASS_ACTIVITY_DATACUBE.getColumnFamily(), CqlQueries.GET_USER_CLASS_ACTIVITY_DATACUBE, rowKey, userUid, collectionType);
		} else if (StringUtils.isNotBlank(collectionType)) {

			resultRows = getCassandraService().readRows(ColumnFamily.CLASS_ACTIVITY_DATACUBE.getColumnFamily(), CqlQueries.GET_CLASS_ACTIVITY_DATACUBE, rowKey, collectionType);
		}
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
		Map<String, Object> dataAsMap = new HashMap<String, Object>(8);
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
	
	public ResponseParamDTO<Map<String, Object>> getAllStudentsPerformance(String classId, String courseId, String unitId, String lessonId, String gooruOid, String collectionType, String userUid) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> resultDataAsList = new ArrayList<Map<String, Object>>(); 
		//Fetch student Latest session for the class
		Map<String, String> userSessions = getUsersLatestSessionId(classId, courseId, unitId, lessonId, gooruOid, userUid);
		for (String sessionId : userSessions.keySet()) {
			Map<String, Object> usageData = new HashMap<String, Object>(2);
			List<Map<String, Object>> sessionActivities = new ArrayList<Map<String, Object>>();
			usageData.put(ApiConstants.USER_UID, userSessions.get(sessionId));
			usageData.put(ApiConstants.USAGE_DATA, sessionActivities);
			// Fetch Usage Data
			if (StringUtils.isNotBlank(sessionId)) {
				getResourceMetricsBySession(sessionActivities, sessionId, new HashMap<String, Object>(2));
			}
			resultDataAsList.add(usageData);
		}
		responseParamDTO.setContent(resultDataAsList);
		return responseParamDTO;
	}

	private List<Map<String, Object>> getPriorUsage(String sessionKey) {
	
		List<Map<String, Object>> sessionActivities = new ArrayList<Map<String,Object>>();
		CqlResult<String, String> userSessionActivityResult = getCassandraService().readRows(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(), CqlQueries.GET_USER_SESSION_ACTIVITY, sessionKey);
		if (userSessionActivityResult != null && userSessionActivityResult.hasRows()) {
			for (Row<String, String> userSessionActivityRow : userSessionActivityResult.getRows()) {
				Map<String, Object> sessionActivityMetrics = new HashMap<String, Object>();
				ColumnList<String> sessionActivityColumns = userSessionActivityRow.getColumns();
				String contentType = sessionActivityColumns.getStringValue(ApiConstants._RESOURCE_TYPE, ApiConstants.STRING_EMPTY);
				if(contentType.matches(ApiConstants.COLLECTION_OR_ASSESSMENT)) {
					continue;
				} else {
					sessionActivityMetrics.put(ApiConstants.GOORUOID, sessionActivityColumns.getStringValue(ApiConstants._GOORU_OID, null));
					sessionActivityMetrics.put(ApiConstants.ANSWER_OBJECT, ServiceUtils.castJSONToList(sessionActivityColumns.getStringValue(ApiConstants._ANSWER_OBJECT, ApiConstants.NA)));
					sessionActivityMetrics.put(ApiConstants.VIEWS, sessionActivityColumns.getLongValue(ApiConstants.VIEWS, 0L));
					sessionActivityMetrics.put(ApiConstants.REACTION, sessionActivityColumns.getLongValue(ApiConstants.REACTION, 0L));
					sessionActivities.add(sessionActivityMetrics);
				}
			}
		}
		return sessionActivities;
	}
	private void getResourceMetricsBySession(List<Map<String, Object>> sessionActivities, String sessionKey, Map<String, Object> usageData) {
		
		CqlResult<String, String> userSessionActivityResult = getCassandraService().readRows(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(), CqlQueries.GET_USER_SESSION_ACTIVITY,  sessionKey);
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
					sessionActivityMetrics.put(ApiConstants.ANSWER_OBJECT, ServiceUtils.castJSONToList(sessionActivityColumns.getStringValue(ApiConstants._ANSWER_OBJECT, ApiConstants.NA)));
					sessionActivities.add(sessionActivityMetrics);
				}
			}
			usageData.put(itemName, sessionActivities);
		}
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
			activityList.addAll(getDomainActivity(studentId, subjectId, courseId, domainId).getContent());
		}
		responseParamDTO.setContent(activityList);
		return responseParamDTO;
	}
	
	
	public Observable<ResponseParamDTO<ContentTaxonomyActivity>> getTaxonomyActivity(Integer depth, String... taxonomyLevelId) {

		Observable<ResponseParamDTO<ContentTaxonomyActivity>> observable = Observable.<ResponseParamDTO<ContentTaxonomyActivity>> create(s -> {
			try {
				switch(depth) {
				case 1: 
					s.onNext(getSubjectActivity(taxonomyLevelId[0], taxonomyLevelId[1]));
					break;
				case 2:
					s.onNext(getCourseActivity(taxonomyLevelId[0], taxonomyLevelId[1], taxonomyLevelId[2]));
					break;
				case 3:
					s.onNext(getDomainActivity(taxonomyLevelId[0], taxonomyLevelId[1], taxonomyLevelId[2], taxonomyLevelId[3]));
					break;
				case 4:
					s.onNext(getStandardActivity(taxonomyLevelId[0], taxonomyLevelId[1], taxonomyLevelId[2], taxonomyLevelId[3], taxonomyLevelId[4]));
					break;
				}
			} catch (Throwable t) {
				s.onError(t);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getSubjectActivity(String studentId, String subjectId) {

		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		CqlResult<String, String> userSubjectActivityResult = getCassandraService().readRows(ColumnFamily.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily(), CqlQueries.GET_SUBJECT_ACTIVITY, studentId, subjectId);
		Map<String, Set<String>> itemMap = new HashMap<String, Set<String>>();
		if (userSubjectActivityResult != null && userSubjectActivityResult.hasRows()) {
			for (Row<String, String> row : userSubjectActivityResult.getRows()) {
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				ColumnList<String> subjectUsage = row.getColumns();
				String courseId =  subjectUsage.getStringValue(ApiConstants._COURSE_ID, null);
				String domainId =  subjectUsage.getStringValue(ApiConstants._DOMAIN_ID, null);
				contentTaxonomyActivity.setCourseId(courseId);
				itemMap = childActivityMetrics(contentTaxonomyActivity, subjectUsage, courseId, domainId);
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
		}
		contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, 1);
		includeActiveChildCount(contentTaxonomyActivityList, itemMap, 1);
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getCourseActivity(String studentId, String subjectId, String courseId) {
		
		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		CqlResult<String, String> userCourseActivityResult = getCassandraService().readRows(ColumnFamily.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily(), CqlQueries.GET_COURSE_ACTIVITY, studentId, subjectId, courseId);
		Map<String, Set<String>> itemMap = new HashMap<String, Set<String>>();
		if (userCourseActivityResult != null && userCourseActivityResult.hasRows()) {
			for (Row<String, String> row : userCourseActivityResult.getRows()) {
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				ColumnList<String> courseUsage = row.getColumns();
				String domainId = courseUsage.getStringValue(ApiConstants._DOMAIN_ID, null);
				String standardsId = courseUsage.getStringValue(ApiConstants._STANDARDS_ID, null);
				contentTaxonomyActivity.setDomainId(domainId);
				itemMap = childActivityMetrics(contentTaxonomyActivity, courseUsage, domainId, standardsId);
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
		}
		contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, 2);
		includeActiveChildCount(contentTaxonomyActivityList, itemMap, 2);
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getDomainActivity(String studentId, String subjectId, String courseId, String domainId) {
		
		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		CqlResult<String, String> userDomainActivityResult = getCassandraService().readRows(ColumnFamily.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily(), CqlQueries.GET_DOMAIN_ACTIVITY, studentId, subjectId, courseId, domainId);
		Map<String, Set<String>> itemMap = new HashMap<String, Set<String>>();
		if (userDomainActivityResult != null && userDomainActivityResult.hasRows()) {
			for (Row<String, String> row : userDomainActivityResult.getRows()) {
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				ColumnList<String> domainUsage = row.getColumns();
				String standardsId = domainUsage.getStringValue(ApiConstants._STANDARDS_ID, null);
				String learningTargetId = domainUsage.getStringValue(ApiConstants._LEARNING_TARGETS_ID, null);
				contentTaxonomyActivity.setStandardsId(standardsId);
				itemMap = childActivityMetrics(contentTaxonomyActivity, domainUsage, standardsId, learningTargetId);
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
		}
		contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, 3);
		includeActiveChildCount(contentTaxonomyActivityList, itemMap, 3);
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getStandardActivity(String studentId, String subjectId, String courseId, String domainId, String standardsId) {

		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		CqlResult<String, String> userStandardActivityResult = getCassandraService().readRows(ColumnFamily.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily(), CqlQueries.GET_STANDARDS_ACTIVITY, studentId, subjectId, courseId, domainId, standardsId);
		if (userStandardActivityResult != null && userStandardActivityResult.hasRows()) {
			for (Row<String, String> row : userStandardActivityResult.getRows()) {
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				ColumnList<String> standardUsage = row.getColumns();
				String learningTargetId = standardUsage.getStringValue(ApiConstants._LEARNING_TARGETS_ID, null);
				contentTaxonomyActivity.setLearningTargetsId(learningTargetId);
				childActivityMetrics(contentTaxonomyActivity, standardUsage, learningTargetId, null);
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
		}
		contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, 4);
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> fetchTeacherGrade(String teacherUid, String userUid, String sessionId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		CqlResult<String, String> result = getCassandraService().readRows(ColumnFamily.STUDENT_QUESTION_GRADE.getColumnFamily(), CqlQueries.GET_STUDENT_QUESTION_GRADE, teacherUid, userUid, sessionId);
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
			CqlResult<String, String> userSessionActivityResult = getCassandraService().readRows(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(), CqlQueries.GET_USER_SESSION_CONTENT_ACTIVITY, sessionId, resourceId); 
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
		ColumnList<String> resultColumns = getCassandraService().readRow(ColumnFamily.STUDENT_LOCATION.getColumnFamily(), CqlQueries.GET_USER_CURRENT_LOCATION_IN_CLASS, classId, userUid);
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
		CqlResult<String, String> resultRows = getCassandraService().readRows(ColumnFamily.STUDENT_LOCATION.getColumnFamily(), CqlQueries.GET_ALL_USER_CURRENT_LOCATION_IN_CLASS, classId);
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
		CqlResult<String, String> resultRows = getCassandraService().readRows(ColumnFamily.STUDENT_LOCATION.getColumnFamily(), CqlQueries.GET_ALL_USER_CURRENT_LOCATION_IN_CLASS, classId);
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
	
	private Map<String,Set<String>> childActivityMetrics(ContentTaxonomyActivity contentTaxonomyActivity, ColumnList<String> taxonomyUsage, String parentKey, String childKey) {
		
		Map<String,Set<String>> itemMap = new HashMap<>();
		contentTaxonomyActivity.setResourceType(taxonomyUsage.getStringValue(ApiConstants._RESOURCE_TYPE, null));
		if(ApiConstants.QUESTION.equalsIgnoreCase(contentTaxonomyActivity.getResourceType())) {
			contentTaxonomyActivity.setScore(taxonomyUsage.getLongValue(ApiConstants.SCORE, 0L));
			contentTaxonomyActivity.setAttempts(taxonomyUsage.getLongValue(ApiConstants.VIEWS, 0L));
		} else {
			contentTaxonomyActivity.setTimespent(taxonomyUsage.getLongValue(ApiConstants.TIME_SPENT, 0L));
		}
		if(childKey != null) {
			if(itemMap.containsKey(parentKey)) {
				itemMap.get(parentKey).add(childKey);
			} else {
				Set<String> childItems = new HashSet<String>();
				childItems.add(childKey);
				itemMap.put(parentKey, childItems);
			}
		}
		return itemMap;
	}
	
	private void includeActiveChildCount(List<ContentTaxonomyActivity> taxonomyActivities, Map<String,Set<String>> itemMap, Integer depth) {
		
		for(ContentTaxonomyActivity contentTaxonomyActivity : taxonomyActivities) {
			switch(depth) {
			case 1:
				contentTaxonomyActivity.setItemCount(getItemCount(contentTaxonomyActivity.getCourseId(), itemMap));
				break;
			case 2:
				contentTaxonomyActivity.setItemCount(getItemCount(contentTaxonomyActivity.getDomainId(), itemMap));
				break;
			case 3:
				contentTaxonomyActivity.setItemCount(getItemCount(contentTaxonomyActivity.getStandardsId(), itemMap));
				break;
			case 4:
				contentTaxonomyActivity.setItemCount(getItemCount(contentTaxonomyActivity.getLearningTargetsId(), itemMap));
				break;
			}
		}
	}
	
	private int getItemCount(String id, Map<String,Set<String>> itemMap) {
		if(itemMap.containsKey(id)) {
			return itemMap.get(id).size();
		} else {
			return 0;
		}
	}
	
	private Map<String, String> getUsersLatestSessionId(String classId, String courseId, String unitId, String lessonId, String collectionId, String userId) {
		
		Map<String,String> usersSession = new HashMap<String,String>();
		CqlResult<String, String>  usersLatestSession = null;
		if(StringUtils.isNotBlank(userId)) {
			usersLatestSession = cassandraService.readRows(ColumnFamily.USER_CLASS_COLLECTION_LAST_SESSIONS.getColumnFamily(), CqlQueries.GET_USER_CLASS_CONTENT_LATEST_SESSION, classId, courseId, unitId, lessonId, collectionId, userId);
		} else {
			usersLatestSession = cassandraService.readRows(ColumnFamily.USER_CLASS_COLLECTION_LAST_SESSIONS.getColumnFamily(), CqlQueries.GET_USERS_CLASS_CONTENT_LATEST_SESSION, classId, courseId, unitId, lessonId, collectionId);
		}
		for(Row<String, String> row : usersLatestSession.getRows()) {
			ColumnList<String> columnList = row.getColumns();
			usersSession.put(columnList.getStringValue(ApiConstants._SESSION_ID, null), columnList.getStringValue(ApiConstants._USER_UID, null));
		}
		return usersSession;
	}
} 
