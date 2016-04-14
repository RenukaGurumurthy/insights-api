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

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.CqlQueries;
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.models.StudentsClassActivity;
import org.gooru.insights.api.models.UserContentLocation;
import org.gooru.insights.api.utils.ServiceUtils;
import org.gooru.insights.api.utils.ValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import rx.Observable;
import rx.schedulers.Schedulers;

@Service
public class ClassServiceImpl implements ClassService {
	
	private static final Logger LOG = LoggerFactory.getLogger(ClassServiceImpl.class);

    private final ExecutorService observableExecutor = Executors.newFixedThreadPool(10);
    
	@Autowired
	private CassandraService cassandraService;
	
	@Autowired
	private LambdaService lambdaService;

	private CassandraService getCassandraService() {
		return cassandraService;
	}

	public Observable<ResponseParamDTO<Map<String, Object>>> getSessionStatus(String sessionId, String contentGooruId) {
		
		Observable<ResponseParamDTO<Map<String, Object>>> sessionStatus = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(fetchSessionStatus(sessionId, contentGooruId));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return sessionStatus;
	}
	
	private ResponseParamDTO<Map<String, Object>> fetchSessionStatus(String sessionId, String contentGooruId) {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		//CqlResult<String, String> sessionDetails = getCassandraService().readRows(ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily(), CqlQueries.GET_SESSION_ACTIVITY_TYPE, sessionId, contentGooruId);
		ResultSet sessionDetails = getCassandraService().getSessionActivityType(sessionId, contentGooruId);
		if (sessionDetails != null) {
			for(Row row : sessionDetails) {
				Map<String, Object> sessionDataMap = new HashMap<String, Object>();
				String status = row.getString(ApiConstants._EVENT_TYPE);
				sessionDataMap.put(ApiConstants.SESSIONID, sessionId);
				status = status.equalsIgnoreCase(ApiConstants.STOP) ? ApiConstants.COMPLETED : ApiConstants.INPROGRESS;
				sessionDataMap.put(ApiConstants.STATUS, status);
				responseParamDTO.setMessage(sessionDataMap);
			}
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E110, sessionId);
		}
		return responseParamDTO;
	}

	public Observable<ResponseParamDTO<Map<String, Object>>> getUserSessions(String classId, String courseId, String unitId,
			String lessonId, String collectionId, String collectionType, String userUid, boolean fetchOpenSession) {
		
		Observable<ResponseParamDTO<Map<String, Object>>> userSessions = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			try {
				s.onNext(fetchUserSessions(classId, courseId, unitId,
						lessonId, collectionId, collectionType, userUid, fetchOpenSession));
			} catch (Exception e) {
				s.onError(e);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return userSessions;
	}

	private ResponseParamDTO<Map<String, Object>> fetchUserSessions(String classId, String courseId, String unitId,
			String lessonId, String collectionId, String collectionType, String userUid, boolean fetchOpenSession) throws Exception {

		// TODO Enabled for class verification
		// isValidClass(classId);
		List<Map<String, Object>> resultSet =  null;
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
			if(collectionType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) {
				if(fetchOpenSession) {
					resultSet = getSessionInfo(CqlQueries.GET_USER_ASSESSMENT_SESSIONS, userUid, collectionId, collectionType, classId, courseId, unitId, lessonId, ApiConstants.START);
				} else {
					resultSet = getSessionInfo(CqlQueries.GET_USER_ASSESSMENT_SESSIONS, userUid, collectionId, collectionType, classId, courseId, unitId, lessonId, ApiConstants.STOP);
				}
			} else {
				resultSet = getSessionInfo(CqlQueries.GET_USER_COLLECTION_SESSIONS, userUid, collectionId, collectionType, classId, courseId, unitId, lessonId,null);
			}
		resultSet = ServiceUtils.sortBy(resultSet, ApiConstants.EVENT_TIME, ApiConstants.ASC);
		responseParamDTO.setContent(addSequence(resultSet));
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> getSessionInfo(String query, String userUid, String collectionUid, String collectionType, String classUid, String courseUid, String unitUid, String lessonUid, String eventType) {
		
		//CqlResult<String, String> sessions = getCassandraService().readRows(ColumnFamilySet.USER_SESSIONS.getColumnFamily(), query, parameters);
		ResultSet sessions = getCassandraService().getUserAssessmentSessions(userUid, collectionUid, collectionType, classUid, courseUid, unitUid, lessonUid, eventType);
		List<Map<String,Object>> sessionList = new ArrayList<Map<String,Object>>();
		if( sessions != null) {
			for(Row row : sessions) {
					Map<String, Object> sessionMap = new HashMap<String,Object>();
					sessionMap.put(ApiConstants.SESSIONID,row.getString(ApiConstants._SESSION_ID));
					sessionMap.put(ApiConstants.EVENT_TIME,row.getLong(ApiConstants._EVENT_TIME));
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
				resultMap.put(ApiConstants.SEQUENCE, sequence++);
				finalSet.add(resultMap);
			}
		}
		return finalSet;
	}
	
	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getUserCurrentLocationInLesson(String userUid, String classId) {
		Observable<ResponseParamDTO<Map<String, Object>>> userLocationObservable = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getStudentCurrentLocation(userUid, classId));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return userLocationObservable;
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
		String rowKey = ServiceUtils.appendTilda(classId, courseId, unitId, lessonId);
		//CqlResult<String, String> result = getCassandraService().readRows(ColumnFamilySet.CLASS_ACTIVITY_PEER_DETAIL.getColumnFamily(), CqlQueries.GET_USER_PEER_DETAIL, rowKey);
		ResultSet result = getCassandraService().getUserPeerDetail(rowKey);
		if (result != null ) {
			for(Row resultRow : result) {
				Map<String, Object> dataAsMap = new HashMap<String, Object>(5);
				Set<String> activePeers = resultRow.getSet(ApiConstants._ACTIVE_PEERS, String.class);
				Set<String> leftPeers = resultRow.getSet(ApiConstants._LEFT_PEERS, String.class);
				dataAsMap.put(ApiConstants.ACTIVE_PEER_COUNT, leftPeers.size());
				dataAsMap.put(ApiConstants.LEFT_PEER_COUNT, activePeers.size());
				nextLevelType = resultRow.getString(ApiConstants._COLLECTION_TYPE);
				if(nextLevelType.matches(ApiConstants.COLLECTION_OR_ASSESSMENT)) {
					dataAsMap.put(ApiConstants.ACTIVE_PEER_UIDS, leftPeers);
					dataAsMap.put(ApiConstants.LEFT_PEER_UIDS, activePeers);
				}
				dataAsMap.put(ApiConstants.getResponseNameByType(nextLevelType), resultRow.getString(ApiConstants._LEAF_GOORU_OID));
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
			ResponseParamDTO<Map<String, Object>> sessionObject = fetchUserSessions(classId, courseId, unitId,lessonId, assessmentId, collectionType, userUid, openSession);
			List<Map<String,Object>> sessionList = sessionObject.getContent();
			return  sessionList.size() > 0 ? sessionList.get(sessionList.size()-1).get(ApiConstants.SESSIONID).toString() : null;
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E111, ServiceUtils.appendComma("CUL Heirarchy", ApiConstants.SESSIONID)
					, ServiceUtils.appendComma("CUL Heirarchy", ApiConstants.SESSIONID));
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private ResponseParamDTO<Map<String, Object>> getPerformanceData(String classId, String courseId, String unitId, String lessonId, String userUid, String collectionType, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		String rowKey = ServiceUtils.appendTilda(classId, courseId, unitId, lessonId);
		ResultSet resultRows = null;
		if (StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(collectionType)) {
			//resultRows = getCassandraService().readRows(ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily(), CqlQueries.GET_USER_CLASS_ACTIVITY_DATACUBE, rowKey, userUid, collectionType);
			resultRows = getCassandraService().getUserClassActivityDatacube(rowKey, userUid, collectionType);
		} else if (StringUtils.isNotBlank(collectionType)) {
			//resultRows = getCassandraService().readRows(ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily(), CqlQueries.GET_CLASS_ACTIVITY_DATACUBE, rowKey, collectionType);
			resultRows = getCassandraService().getClassActivityDatacube(rowKey, collectionType);
		}
		if (resultRows != null) {
			Map<String, Object> userUsageAsMap = new HashMap<String, Object>();
			for (Row resultRow : resultRows) {
				List<Map<String, Object>> dataMapList = new ArrayList<Map<String, Object>>();
				String userId = resultRow.getString(ApiConstants._USER_UID);
				if (userUsageAsMap.containsKey(userId) && userUsageAsMap.get(userId) != null) {
					dataMapList = (List<Map<String, Object>>) userUsageAsMap.get(userId);
				}
				addPerformanceMetrics(classId, lessonId, dataMapList, resultRow, collectionType, nextLevelType);
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

	private ResponseParamDTO<Map<String, Object>> getPerformanceDataByLambda(String classId, String courseId,
			String unitId, String lessonId, String userUid, String collectionType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		String nextLevel = null;
		if (courseId != null && unitId == null) {
			nextLevel = ApiConstants.UNIT;
		} else if (unitId != null && lessonId == null) {
			nextLevel = ApiConstants.LESSON;
		} else if (unitId != null && lessonId != null) {
			nextLevel = ApiConstants.COLLECTION;
		} else {
			nextLevel = ApiConstants.COLLECTION;
		}
		List<StudentsClassActivity> classActivityResultSetList = new ArrayList<StudentsClassActivity>();
		ResultSet classActivityResultSet = getCassandraService().getStudentsClassActivity(classId, courseId, unitId,
				lessonId, null);

		for (Row classActivityRow : classActivityResultSet) {
			StudentsClassActivity studentsClassActivity = new StudentsClassActivity();
			switch (nextLevel) {
			case ApiConstants.UNIT:
				studentsClassActivity.setUnitUid(classActivityRow.getString(ApiConstants._UNIT_UID));
				studentsClassActivity.setLessonUid(classActivityRow.getString(ApiConstants._LESSON_UID));
				break;
			case ApiConstants.LESSON:
				studentsClassActivity.setLessonUid(classActivityRow.getString(ApiConstants._LESSON_UID));
				break;
			default:
				LOG.debug("Do nothing in collection/assessment level");
				break;
			}
			studentsClassActivity.setUserUid(classActivityRow.getString(ApiConstants._USER_UID));
			studentsClassActivity.setCollectionUid(classActivityRow.getString(ApiConstants._COLLECTION_UID));
			studentsClassActivity.setCollectionType(classActivityRow.getString(ApiConstants._COLLECTION_TYPE));
			studentsClassActivity.setScore(classActivityRow.getLong(ApiConstants.SCORE));
			studentsClassActivity.setReaction(classActivityRow.getLong(ApiConstants.REACTION));
			studentsClassActivity.setViews(classActivityRow.getLong(ApiConstants.VIEWS));
			studentsClassActivity.setTimeSpent(classActivityRow.getLong(ApiConstants._TIME_SPENT));
			studentsClassActivity.setAttemptStatus(classActivityRow.getString(ApiConstants._ATTEMPT_STATUS));
			classActivityResultSetList.add(studentsClassActivity);
		}
		List<StudentsClassActivity> filteredList = lambdaService
				.applyFiltersInStudentsClassActivity(classActivityResultSetList, collectionType);
		List<List<StudentsClassActivity>> aggregatedList = lambdaService
				.aggregateStudentsClassActivityData(filteredList, nextLevel);

		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		for (List<StudentsClassActivity> aggregatedSubList : aggregatedList) {
			Map<String, Object> resultMap = new HashMap<String, Object>();
			List<StudentsClassActivity> orderedList = new ArrayList<StudentsClassActivity>();
			for (StudentsClassActivity subObject : aggregatedSubList) {
				orderedList.add(subObject);
				resultMap.put("userUId", subObject.getUserUid());
			}
			resultMap.put("usageData", orderedList);
			result.add(resultMap);
		}
		responseParamDTO.setContent(result);
		return responseParamDTO;
	}

	//TODO nextLevelType is hard coded temporarily. In future, store and get nextLevelType from CF
	private void addPerformanceMetrics(String classId, String lessonId, List<Map<String, Object>> dataMapList, Row resultRow, String collectionType, String nextLevelType) {
		Map<String, Object> dataAsMap = new HashMap<String, Object>(8);
		String responseNameForViews = ApiConstants.VIEWS;
		if(nextLevelType.equalsIgnoreCase(ApiConstants.CONTENT)) {
			nextLevelType = collectionType;
			dataAsMap.put(ApiConstants.REACTION, resultRow.getLong(ApiConstants.REACTION));
		}
		if(collectionType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) responseNameForViews = ApiConstants.ATTEMPTS;
		String leafNodeId = resultRow.getString(ApiConstants._LEAF_NODE);
		if(lessonId != null) leafNodeId = lessonId;
		dataAsMap.put(ApiConstants.getResponseNameByType(nextLevelType), resultRow.getString(ApiConstants._LEAF_NODE));
		dataAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, resultRow.getLong(ApiConstants.SCORE));
		dataAsMap.put(responseNameForViews, resultRow.getLong(ApiConstants.VIEWS));
		dataAsMap.put(ApiConstants.TIMESPENT, resultRow.getLong(ApiConstants._TIME_SPENT));
		dataAsMap.put(ApiConstants.COMPLETED_COUNT, resultRow.getLong(ApiConstants._COMPLETED_COUNT));
		//TODO Need to add logic to fetch total count meta data from Database
		dataAsMap.put(ApiConstants.TOTAL_COUNT, getCulCollectionCount(classId, leafNodeId, collectionType));
		dataMapList.add(dataAsMap);
	}
	
	private ResponseParamDTO<Map<String, Object>> getAllStudentsPerformance(String classId, String courseId, String unitId, String lessonId, String gooruOid, String collectionType, String userUid) throws Exception {
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
		ResultSet userSessionActivityResult = getCassandraService().getUserSessionActivity(sessionKey);
		if (userSessionActivityResult != null) {
			for (Row userSessionActivityRow : userSessionActivityResult) {
				Map<String, Object> sessionActivityMetrics = new HashMap<String, Object>();
				String contentType = userSessionActivityRow.getString(ApiConstants._RESOURCE_TYPE);
				if(contentType.matches(ApiConstants.COLLECTION_OR_ASSESSMENT)) {
					continue;
				} else {
					sessionActivityMetrics.put(ApiConstants.PARENT_EVENT_ID, userSessionActivityRow.getString(ApiConstants._PARENT_EVENT_ID));
					sessionActivityMetrics.put(ApiConstants.GOORUOID, userSessionActivityRow.getString(ApiConstants._GOORU_OID));
					sessionActivityMetrics.put(ApiConstants.ANSWER_OBJECT, ServiceUtils.castJSONToList(userSessionActivityRow.getString(ApiConstants._ANSWER_OBJECT)));
					sessionActivityMetrics.put(ApiConstants.VIEWS, userSessionActivityRow.getLong(ApiConstants.VIEWS));
					sessionActivityMetrics.put(ApiConstants.REACTION, userSessionActivityRow.getLong(ApiConstants.REACTION));
					sessionActivities.add(sessionActivityMetrics);
				}
			}
		}
		return sessionActivities;
	}
	private void getResourceMetricsBySession(List<Map<String, Object>> sessionActivities, String sessionKey, Map<String, Object> usageData) {
		
		ResultSet userSessionActivityResult = getCassandraService().getUserSessionActivity(sessionKey);
		if (userSessionActivityResult != null) {
			String itemName = ApiConstants.RESOURCES;
			for (Row userSessionActivityRow : userSessionActivityResult) {
				Map<String, Object> sessionActivityMetrics = new HashMap<String, Object>();
				String contentType = userSessionActivityRow.getString(ApiConstants._RESOURCE_TYPE);
				sessionActivityMetrics.put(ApiConstants.SESSIONID, userSessionActivityRow.getString(ApiConstants._SESSION_ID));
				sessionActivityMetrics.put(ApiConstants.GOORUOID, userSessionActivityRow.getString(ApiConstants._GOORU_OID));
				sessionActivityMetrics.put(ApiConstants.RESOURCE_TYPE, contentType);
				sessionActivityMetrics.put(ApiConstants.SCORE, userSessionActivityRow.getLong(ApiConstants.SCORE));
				sessionActivityMetrics.put(ApiConstants.TIMESPENT, userSessionActivityRow.getLong(ApiConstants._TIME_SPENT));
				sessionActivityMetrics.put(ApiConstants.VIEWS, userSessionActivityRow.getLong(ApiConstants.VIEWS));
				sessionActivityMetrics.put(ApiConstants.REACTION, userSessionActivityRow.getLong(ApiConstants.REACTION));
				if (contentType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
					usageData.put(ApiConstants.COLLECTION, sessionActivityMetrics);
				} else if (contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) {
					usageData.put(ApiConstants.ASSESSMENT, sessionActivityMetrics);
					sessionActivityMetrics.remove(ApiConstants.VIEWS);
					itemName = ApiConstants.QUESTIONS;
					sessionActivityMetrics.put(ApiConstants.ATTEMPTS, userSessionActivityRow.getLong(ApiConstants.VIEWS));
				} else {
					sessionActivityMetrics.put(ApiConstants.QUESTION_TYPE, userSessionActivityRow.getString(ApiConstants._QUESTION_TYPE));
					sessionActivityMetrics.put(ApiConstants.ANSWER_OBJECT, ServiceUtils.castJSONToList(userSessionActivityRow.getString(ApiConstants._ANSWER_OBJECT)));
					sessionActivities.add(sessionActivityMetrics);
				}
			}
			usageData.put(itemName, sessionActivities);
		}
	}

	public Observable<ResponseParamDTO<ContentTaxonomyActivity>> getUserDomainParentMastery(String studentId, String domainIds) {
	
		Observable<ResponseParamDTO<ContentTaxonomyActivity>> observable = Observable.<ResponseParamDTO<ContentTaxonomyActivity>> create(s -> {
			try {
					s.onNext(getUserDomainParentMasteryUsage(studentId, domainIds));		
			} catch (Throwable t) {
				s.onError(t);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return observable;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getUserDomainParentMasteryUsage(String studentId, String domainIds) {
		
		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		
		if(StringUtils.isEmpty(domainIds)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E104, ApiConstants.COURSE_IDS);
		}
		ResultSet taxonomyParents = getCassandraService().getTaxonomyParents(domainIds);
		if(taxonomyParents != null) {
			for(Row row : taxonomyParents) {
				String subjectId = row.getString("subject_id");
				String courseId = row.getString("course_id");
				String domainId = row.getString("domain_id");
				if(StringUtils.isNotBlank(subjectId) && StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(domainId)) {
				responseParamDTO.setContent(getDomainParentActivity(studentId, subjectId, courseId, domainId).getContent());
				}
			}
		}
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
		ResultSet userSubjectActivityResult = getCassandraService().getSubjectActivity(studentId, subjectId);
		Map<String, Set<String>> itemMap = new HashMap<String, Set<String>>();
		if (userSubjectActivityResult != null) {
			for (Row row : userSubjectActivityResult) {
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				String courseId =  row.getString(ApiConstants._COURSE_ID);
				String domainId =  row.getString(ApiConstants._DOMAIN_ID);
				contentTaxonomyActivity.setCourseId(courseId);
				childActivityMetrics(contentTaxonomyActivity, row, itemMap, courseId, domainId);
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
		}
		contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, 1);
		includeAdditionalMetrics(contentTaxonomyActivityList, itemMap, 1);
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getCourseActivity(String studentId, String subjectId, String courseId) {
		
		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		ResultSet userCourseActivityResult = getCassandraService().getCourseActivity(studentId, subjectId, courseId);
		Map<String, Set<String>> itemMap = new HashMap<String, Set<String>>();
		if (userCourseActivityResult != null) {
			for (Row row : userCourseActivityResult) {
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				String domainId = row.getString(ApiConstants._DOMAIN_ID);
				String standardsId = row.getString(ApiConstants._STANDARDS_ID);
				contentTaxonomyActivity.setDomainId(domainId);
				childActivityMetrics(contentTaxonomyActivity, row, itemMap, domainId, standardsId);
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
		}
		contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, 2);
		includeAdditionalMetrics(contentTaxonomyActivityList, itemMap, 2);
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getDomainActivity(String studentId, String subjectId, String courseId, String domainId) {
		
		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		ResultSet userDomainActivityResult = getCassandraService().getDomainActivity(studentId, subjectId, courseId, domainId);
		Map<String, Set<String>> itemMap = new HashMap<String, Set<String>>();
		if (userDomainActivityResult != null) {
			for (Row row : userDomainActivityResult) {
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				String standardsId = row.getString(ApiConstants._STANDARDS_ID);
				String learningTargetId = row.getString(ApiConstants._LEARNING_TARGETS_ID);
				contentTaxonomyActivity.setStandardsId(standardsId);
				childActivityMetrics(contentTaxonomyActivity, row, itemMap, standardsId, learningTargetId);
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
		}
		contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, 3);
		includeAdditionalMetrics(contentTaxonomyActivityList, itemMap, 3);
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<ContentTaxonomyActivity> getDomainParentActivity(String studentId, String subjectId, String courseId, String domainId) {
		
		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		ResultSet userDomainActivityResult = getCassandraService().getDomainActivity(studentId, subjectId, courseId, domainId);
		Map<String, Set<String>> itemMap = new HashMap<String, Set<String>>();
		if (userDomainActivityResult != null) {
			for (Row row : userDomainActivityResult) {
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				String standardsId = row.getString(ApiConstants._STANDARDS_ID);
				contentTaxonomyActivity.setDomainId(domainId);
				childActivityMetrics(contentTaxonomyActivity, row, itemMap, domainId, standardsId);
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
		}
		contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, 2);
		includeAdditionalMetrics(contentTaxonomyActivityList, itemMap, 2);
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	private ResponseParamDTO<ContentTaxonomyActivity> getStandardActivity(String studentId, String subjectId, String courseId, String domainId, String standardsId) {

		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<ContentTaxonomyActivity>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		ResultSet userStandardActivityResult = getCassandraService().getStandardsActivity(studentId, subjectId, courseId, domainId, standardsId);
		if (userStandardActivityResult != null) {
			for (Row row : userStandardActivityResult) {
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();
				String learningTargetId = row.getString(ApiConstants._LEARNING_TARGETS_ID);
				contentTaxonomyActivity.setLearningTargetsId(learningTargetId);
				childActivityMetrics(contentTaxonomyActivity, row, null, learningTargetId, null);
				contentTaxonomyActivityList.add(contentTaxonomyActivity);
			}
		}
		contentTaxonomyActivityList = lambdaService.aggregateTaxonomyActivityData(contentTaxonomyActivityList, 4);
		includeAdditionalMetrics(contentTaxonomyActivityList, null, 3);
		responseParamDTO.setContent(contentTaxonomyActivityList);
		return responseParamDTO;
	}
	
	public Observable<ResponseParamDTO<Map<String, Object>>> getTeacherGrade(String teacherUid, String userUid, String sessionId) {
		
		Observable<ResponseParamDTO<Map<String, Object>>> teacherGrade = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(fetchTeacherGrade(teacherUid, userUid, sessionId));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return teacherGrade;
	}
	
	private ResponseParamDTO<Map<String, Object>> fetchTeacherGrade(String teacherUid, String userUid, String sessionId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		if(StringUtils.isBlank(teacherUid)) {
			return responseParamDTO;
		}
		ResultSet result = getCassandraService().getStudentQuestionGrade(teacherUid, userUid, sessionId);
		List<Map<String, Object>> teacherGradeAsList = new ArrayList<>();
		if(result != null) {
			for(Row row : result) {
				Map<String, Object> teacherGradeAsMap = new HashMap<String, Object>();
				teacherGradeAsMap.put(ApiConstants.QUESTION_ID, row.getString(ApiConstants._QUESTION_ID));
				teacherGradeAsMap.put(ApiConstants.SCORE, row.getLong(ApiConstants.SCORE));
				teacherGradeAsList.add(teacherGradeAsMap);
			}
			responseParamDTO.setContent(teacherGradeAsList);
		}
		return responseParamDTO;
	}

	public Observable<ResponseParamDTO<Map<String, Object>>> getResourceUsage(String sessionId, String resourceIds) {
		
		Observable<ResponseParamDTO<Map<String, Object>>> teacherGrade = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(fetchResourceUsage(sessionId, resourceIds));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return teacherGrade;
	}

	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getStatisticalMetrics(String gooruOids) {
		
		Observable<ResponseParamDTO<Map<String, Object>>> teacherGrade = Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getStatMetrics(gooruOids));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
		return teacherGrade;
	}

	private ResponseParamDTO<Map<String, Object>> getStatMetrics(String gooruOids) {
		if (StringUtils.isBlank(gooruOids)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E104, ApiConstants.RESOURCE_IDS);
		}
		ResponseParamDTO<Map<String, Object>> resourceUsageObject = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> resourceUsageList = new ArrayList<Map<String, Object>>();
		for (String resourceId : gooruOids.split(ApiConstants.COMMA)) {
			ResultSet statMetricsResult = getCassandraService().getStatisticalMetrics(resourceId);
			if (statMetricsResult != null) {
				String clusterKey = null;
				Map<String, Object> resourceUsage = null;
				for (Row statMetricsRow : statMetricsResult.all()) {
					String gooruOid = statMetricsRow.getString(ApiConstants._CLUSTERING_KEY);
					if (clusterKey == null || (clusterKey != null && !clusterKey.equalsIgnoreCase(gooruOid))) {
						resourceUsage = new HashMap<String, Object>();
					}
					resourceUsage.put(ApiConstants.GOORUOID, gooruOid);
					resourceUsage.put(statMetricsRow.getString(ApiConstants._METRICS_NAME),
							statMetricsRow.getLong(ApiConstants._METRICS_VALUE));
					clusterKey = gooruOid;
				}
				if (resourceUsage != null && !resourceUsage.isEmpty()) {
					resourceUsageList.add(resourceUsage);
				}
			}
		}
		resourceUsageObject.setContent(resourceUsageList);
		return resourceUsageObject;
	}
	
	private ResponseParamDTO<Map<String, Object>> fetchResourceUsage(String sessionId, String resourceIds) {
		
		if(StringUtils.isBlank(resourceIds)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E104, ApiConstants.RESOURCE_IDS);	
		}
		ResponseParamDTO<Map<String, Object>> resourceUsageObject = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String,Object>> resourceUsageList = new ArrayList<Map<String,Object>>();
		for(String resourceId : resourceIds.split(ApiConstants.COMMA)) {
			ResultSet userSessionActivityResult = getCassandraService().getUserSessionContentActivity(sessionId, resourceId.trim()); 
			if(userSessionActivityResult != null) {
				for(Row userSessionColumn : userSessionActivityResult) {
					Map<String, Object> resourceUsage = new HashMap<String,Object>();
					resourceUsage.put(ApiConstants.GOORUOID, userSessionColumn.getString(ApiConstants._GOORU_OID));
					resourceUsage.put(ApiConstants.RESOURCE_TYPE, userSessionColumn.getString(ApiConstants._RESOURCE_TYPE));
					resourceUsage.put(ApiConstants.QUESTION_TYPE, userSessionColumn.getString(ApiConstants._QUESTION_TYPE));
					resourceUsage.put(ApiConstants.ANSWER_OBJECT, userSessionColumn.getString(ApiConstants._ANSWER_OBJECT));
					resourceUsage.put(ApiConstants.STATUS, userSessionColumn.getString(ApiConstants.ANSWER_STATUS));
					resourceUsage.put(ApiConstants.VIEWS, userSessionColumn.getLong(ApiConstants.VIEWS));
					resourceUsage.put(ApiConstants.TIMESPENT, userSessionColumn.getLong(ApiConstants._TIME_SPENT));
					resourceUsage.put(ApiConstants.SCORE, userSessionColumn.getLong(ApiConstants.SCORE));
					resourceUsage.put(ApiConstants.ATTEMPTS, userSessionColumn.getLong(ApiConstants.ATTEMPTS));
					resourceUsage.put(ApiConstants.REACTION, userSessionColumn.getLong(ApiConstants.REACTION));
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
		ResultSet resultRows = getCassandraService().getUserCurrentLocationInClass(classId, userUid);
		if (resultRows != null) {
			for(Row resultColumns : resultRows){	
			Map<String, Object> dataAsMap = new HashMap<String, Object>();
			dataAsMap.put(ApiConstants.getResponseNameByType(ApiConstants.CLASS), resultColumns.getString(ApiConstants._CLASS_UID));
			dataAsMap.put(ApiConstants.getResponseNameByType(ApiConstants.COURSE), resultColumns.getString(ApiConstants._COURSE_UID));
			dataAsMap.put(ApiConstants.getResponseNameByType(ApiConstants.UNIT), resultColumns.getString(ApiConstants._UNIT_UID));
			dataAsMap.put(ApiConstants.getResponseNameByType(ApiConstants.LESSON), resultColumns.getString(ApiConstants._LESSON_UID));
			dataAsMap.put(ApiConstants.getResponseNameByType(resultColumns.getString(ApiConstants._COLLECTION_TYPE)), resultColumns.getString(ApiConstants._COLLECTION_UID));
			dataMapAsList.add(dataAsMap);
			}
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}
	
	private ResponseParamDTO<Map<String, Object>> getUserPeerData(String classId, String courseId, String unitId, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		List<UserContentLocation> userContentLocationObject = new ArrayList<>();
		ResultSet resultRows = getCassandraService().getAllUserCurrentLocationInClass(classId);
		if (resultRows != null) {
			for (Row columnList : resultRows) {
				String courseUId = columnList.getString(ApiConstants._COURSE_UID);
				String unitUId = columnList.getString(ApiConstants._UNIT_UID);
				String lessonUId = columnList.getString(ApiConstants._LESSON_UID);
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
		ResultSet resultRows = getCassandraService().getAllUserCurrentLocationInClass(classId);
		if (resultRows != null) {
			for (Row columnList : resultRows) {
				String courseUId = columnList.getString(ApiConstants._COURSE_UID); String unitUId = columnList.getString(ApiConstants._UNIT_UID);
				String lessonUId = columnList.getString(ApiConstants._LESSON_UID); String collectionUId = columnList.getString(ApiConstants._COLLECTION_UID);
				nextLevelType = columnList.getString(ApiConstants._COLLECTION_TYPE);
				if (courseUId.equalsIgnoreCase(courseId) && unitUId.equalsIgnoreCase(unitId) && lessonUId.equalsIgnoreCase(lessonId) && StringUtils.isNotBlank(collectionUId)) {
					String status = ApiConstants.IN_ACTIVE; String userId = columnList.getString(ApiConstants._USER_UID); Long sessionTime = columnList.getLong(ApiConstants._SESSION_TIME);
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
	
	private void generateUserLocationObject(String classId, String nextLevelType, List<UserContentLocation> userContentLocationObject, Row columnList, String courseUId, String unitUId,
			String lessonUId) {
		String userId = columnList.getString(ApiConstants._USER_UID);
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
	
	private void childActivityMetrics(ContentTaxonomyActivity contentTaxonomyActivity, Row taxonomyUsage, Map<String,Set<String>> itemMap, String parentKey, String childKey) {
		
		if(ApiConstants.QUESTION.equalsIgnoreCase(taxonomyUsage.getString(ApiConstants._RESOURCE_TYPE))) {
			contentTaxonomyActivity.setScore(taxonomyUsage.getLong(ApiConstants.SCORE));
			contentTaxonomyActivity.setAttempts(taxonomyUsage.getLong(ApiConstants.VIEWS));
		} else {
			contentTaxonomyActivity.setTimespent(taxonomyUsage.getLong(ApiConstants._TIME_SPENT));
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
	}
	
	private void includeAdditionalMetrics(List<ContentTaxonomyActivity> taxonomyActivities, Map<String,Set<String>> attemptedItemMap, Integer depth) {
		
		Map<String, Long> itemCount = new HashMap<String,Long>();
		if(attemptedItemMap != null ){
			Set<String> childIds = attemptedItemMap.keySet();
			ResultSet childItemsCount = getCassandraService().getTaxonomyItemCount(childIds);
			if(childItemsCount != null) {
				for(Row row : childItemsCount) {
					itemCount.put(row.getString("row_key"), row.getLong("item_count"));
				}
			}
		}
		for(ContentTaxonomyActivity contentTaxonomyActivity : taxonomyActivities) {
			
			calculateScoreInPercentage(contentTaxonomyActivity);
			switch(depth) {
			case 1:
				contentTaxonomyActivity.setAttemptedItemCount(getAttemptedItemCount(contentTaxonomyActivity.getCourseId(), attemptedItemMap));
				contentTaxonomyActivity.setItemCount(itemCount.get(contentTaxonomyActivity.getCourseId()));
				break;
			case 2:
				contentTaxonomyActivity.setAttemptedItemCount(getAttemptedItemCount(contentTaxonomyActivity.getDomainId(), attemptedItemMap));
				contentTaxonomyActivity.setItemCount(itemCount.get(contentTaxonomyActivity.getDomainId()));
				break;
			case 3:
				contentTaxonomyActivity.setAttemptedItemCount(getAttemptedItemCount(contentTaxonomyActivity.getStandardsId(), attemptedItemMap));
				contentTaxonomyActivity.setItemCount(itemCount.get(contentTaxonomyActivity.getStandardsId()));
				break;
			case 4:
				contentTaxonomyActivity.setAttemptedItemCount(getAttemptedItemCount(contentTaxonomyActivity.getLearningTargetsId(), attemptedItemMap));
				contentTaxonomyActivity.setItemCount(itemCount.get(contentTaxonomyActivity.getLearningTargetsId()));
				break;
			}
		}
	}
	
	private Long getAttemptedItemCount(String id, Map<String,Set<String>> attemptedItemMap) {
		if(attemptedItemMap == null) {
			return null;
		} else if( attemptedItemMap.containsKey(id)) {
			return attemptedItemMap.get(id).size()+0L;
		} else {
			return 0L;
		}
	}
	
	private void calculateScoreInPercentage(ContentTaxonomyActivity contentTaxonomyActivity) {

		if(contentTaxonomyActivity.getScore() != null && contentTaxonomyActivity.getAttempts() != null) {
			contentTaxonomyActivity.setScoreInPercentage(Math.round((contentTaxonomyActivity.getScore()/contentTaxonomyActivity.getAttempts()))+0L);
			//Avoiding score in response
			contentTaxonomyActivity.setScore(null);
		}
	}
	
	private Map<String, String> getUsersLatestSessionId(String classId, String courseId, String unitId, String lessonId, String collectionId, String userId) {
		
		Map<String,String> usersSession = new HashMap<String,String>();
		ResultSet  usersLatestSession = null;
		if(StringUtils.isNotBlank(userId)) {
			usersLatestSession = cassandraService.getUserClassContentLatestSession(classId, courseId, unitId, lessonId, collectionId, userId);
		} else {
			usersLatestSession = cassandraService.getUsersClassContentLatestSession(classId, courseId, unitId, lessonId, collectionId);
		}
		for(Row columnList : usersLatestSession) {
			usersSession.put(columnList.getString(ApiConstants._SESSION_ID), columnList.getString(ApiConstants._USER_UID));
		}
		return usersSession;
	}

	private long getCulCollectionCount(String classId, String leafNodeId,
			String collectionType) {
		long totalCount = 0L;
		String columnName = ApiConstants.COLLECTION.equals(collectionType) ? ApiConstants._COLLECTION_COUNT
				: ApiConstants._ASSESSMENT_COUNT;
		ResultSet columnList = cassandraService.getClassCollectionCount(
				classId, leafNodeId);
		if (columnList != null) {
			for (Row columns : columnList) {
				totalCount = columns.getLong(columnName);
			}
		}
		return totalCount;
	}
} 
