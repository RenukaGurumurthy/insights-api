package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.gooru.insights.api.models.SessionTaxonomyActivity;
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


		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(fetchSessionStatus(sessionId, contentGooruId));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	private ResponseParamDTO<Map<String, Object>> fetchSessionStatus(String sessionId, String contentGooruId) {

		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		//CqlResult<String, String> sessionDetails = getCassandraService().readRows(ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily(), CqlQueries.GET_SESSION_ACTIVITY_TYPE, sessionId, contentGooruId);

		ResultSet sessionDetails = cassandraService.getSessionActivityType(sessionId, contentGooruId);
		if (sessionDetails != null) {
			for(Row row : sessionDetails) {
				Map<String, Object> sessionDataMap = new HashMap<>();
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

		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			try {
				s.onNext(fetchUserSessions(classId, courseId, unitId,
						lessonId, collectionId, collectionType, userUid, fetchOpenSession));
			} catch (Exception e) {
				s.onError(e);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	private ResponseParamDTO<Map<String, Object>> fetchUserSessions(String classId, String courseId, String unitId,
			String lessonId, String collectionId, String collectionType, String userUid, boolean fetchOpenSession) {

		// TODO Enabled for class verification
		// isValidClass(classId);
		List<Map<String, Object>> resultSet;
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
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

		ResultSet sessions = cassandraService
			.getUserAssessmentSessions(userUid, collectionUid, collectionType, classUid, courseUid, unitUid, lessonUid, eventType);
		List<Map<String,Object>> sessionList = new ArrayList<>();
		if( sessions != null) {
			for(Row row : sessions) {
					Map<String, Object> sessionMap = new HashMap<>();
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
			finalSet = new ArrayList<>();
			for (Map<String, Object> resultMap : resultSet) {
				resultMap.put(ApiConstants.SEQUENCE, sequence++);
				finalSet.add(resultMap);
			}
		}
		return finalSet;
	}

	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getUserCurrentLocationInLesson(String userUid, String classId) {

		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getStudentCurrentLocation(userUid, classId));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getPerformance(String classId, String courseId, String unitId, String lessonId, String userUid, String collectionType, String nextLevelType) {

		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			try {
			s.onNext(getPerformanceDataByLambda(classId, courseId, unitId, lessonId, userUid, collectionType, nextLevelType));
			s.onCompleted();
			}catch(Exception e) {
				s.onError(e);
			}
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getUserPeers(String classId, String courseId, String unitId, String nextLevelType) {

		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getUserPeerData(classId, courseId, unitId, nextLevelType));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getUserPeers(String classId, String courseId, String unitId, String lessonId, String nextLevelType) {

		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getUserPeerData(classId, courseId, unitId, lessonId, nextLevelType));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getAllStudentPerformance(String classId, String courseId, String unitId, String lessonId, String gooruOid, String collectionType, String userUid) {

		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			try {
				s.onNext(getAllStudentsPerformance(classId, courseId, unitId, lessonId, gooruOid, collectionType, userUid));
			} catch (Exception e) {
				s.onError(e);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	@Deprecated
	private ResponseParamDTO<Map<String, Object>> getUserPeersDetail(String classId, String courseId, String unitId, String lessonId, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<>();
		String rowKey = ServiceUtils.appendTilda(classId, courseId, unitId, lessonId);
		//CqlResult<String, String> result = getCassandraService().readRows(ColumnFamilySet.CLASS_ACTIVITY_PEER_DETAIL.getColumnFamily(), CqlQueries.GET_USER_PEER_DETAIL, rowKey);

		ResultSet result = cassandraService.getUserPeerDetail(rowKey);
		if (result != null ) {
			for(Row resultRow : result) {
				Map<String, Object> dataAsMap = new HashMap<>(5);
				Set<String> activePeers = resultRow.getSet(ApiConstants._ACTIVE_PEERS, String.class);
				Set<String> leftPeers = resultRow.getSet(ApiConstants._LEFT_PEERS, String.class);
				dataAsMap.put(ApiConstants.ACTIVE_PEER_COUNT, leftPeers.size());
				dataAsMap.put(ApiConstants.LEFT_PEER_COUNT, activePeers.size());
				nextLevelType = resultRow.getString(ApiConstants._COLLECTION_TYPE);
				if(ApiConstants.COLLECTION_OR_ASSESSMENT_PATTERN.matcher(nextLevelType).matches()) {
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

		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
					try {
						s.onNext(getPriorUsage(classId, courseId, unitId, lessonId, assessmentId, sessionId, userUid,
								collectionType, openSession));
					} catch (Throwable t) {
						s.onError(t);
					}
					s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	private ResponseParamDTO<Map<String, Object>> getPriorUsage(String classId, String courseId, String unitId,
			String lessonId, String assessmentId, String sessionId, String userUid, String collectionType, boolean openSession) throws Exception {

		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
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
			String collectionType) {

		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			try {
				s.onNext(fetchSummaryData(classId, courseId, unitId, lessonId, assessmentId, sessionId, userUid,
						collectionType));
			} catch (Throwable t) {
				s.onError(t);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	private ResponseParamDTO<Map<String, Object>> fetchSummaryData(String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid,
			String collectionType) throws Exception {

		//TODO validate ClassId
		//isValidClass(classId);
		Map<String,Object> usageData = new HashMap<>();
		List<Map<String,Object>> sessionActivities = new ArrayList<>();
		List<Map<String, Object>> summaryData = new ArrayList<>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		String sessionKey = null;
		if (collectionType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) {
			sessionKey = getSession(classId, courseId, unitId, lessonId, assessmentId, sessionId, userUid,
					collectionType, false);
		}else if(collectionType.equalsIgnoreCase(ApiConstants.COLLECTION)){
			sessionKey = ServiceUtils.appendTilda("AS",classId,assessmentId,userUid);
		}
		if (StringUtils.isNotBlank(sessionKey)) {
			//Fetch Usage Data
			getResourceMetricsBySession(sessionActivities, sessionKey, usageData);
			summaryData.add(usageData);
		}
		responseParamDTO.setContent(summaryData);
		return responseParamDTO;
	}

	private String getSession(String classId, String courseId, String unitId, String lessonId, String assessmentId,
			String sessionId, String userUid, String collectionType, boolean openSession) throws Exception {
		if (StringUtils.isNotBlank(sessionId) && !ApiConstants.NA.equalsIgnoreCase(sessionId)) {
			return sessionId;
		} else if (StringUtils.isNotBlank(classId) && StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(unitId)
				&& StringUtils.isNotBlank(lessonId) && !ApiConstants.NA.equalsIgnoreCase(classId)
				&& !ApiConstants.NA.equalsIgnoreCase(courseId) && !ApiConstants.NA.equalsIgnoreCase(unitId)
				&& !ApiConstants.NA.equalsIgnoreCase(lessonId)) {
			ResponseParamDTO<Map<String, Object>> sessionObject = fetchUserSessions(classId, courseId, unitId, lessonId,
					assessmentId, collectionType, userUid, openSession);
			List<Map<String, Object>> sessionList = sessionObject.getContent();
			return sessionList.size() > 0
					? sessionList.get(sessionList.size() - 1).get(ApiConstants.SESSIONID).toString() : null;
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E111,
					ServiceUtils.appendComma("CUL Heirarchy", ApiConstants.SESSIONID),
					ServiceUtils.appendComma("CUL Heirarchy", ApiConstants.SESSIONID));
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private ResponseParamDTO<Map<String, Object>> getPerformanceData(String classId, String courseId, String unitId, String lessonId, String userUid, String collectionType, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<>();
		String rowKey = ServiceUtils.appendTilda(classId, courseId, unitId, lessonId);
		ResultSet resultRows = null;
		if (StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(collectionType)) {
			//resultRows = getCassandraService().readRows(ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily(), CqlQueries.GET_USER_CLASS_ACTIVITY_DATACUBE, rowKey, userUid, collectionType);

			resultRows = cassandraService.getUserClassActivityDatacube(rowKey, userUid, collectionType);
		} else if (StringUtils.isNotBlank(collectionType)) {
			//resultRows = getCassandraService().readRows(ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily(), CqlQueries.GET_CLASS_ACTIVITY_DATACUBE, rowKey, collectionType);

			resultRows = cassandraService.getClassActivityDatacube(rowKey, collectionType);
		}
		if (resultRows != null) {
			Map<String, Object> userUsageAsMap = new HashMap<>();
			for (Row resultRow : resultRows) {
				List<Map<String, Object>> dataMapList = new ArrayList<>();
				String userId = resultRow.getString(ApiConstants._USER_UID);
				if (userUsageAsMap.containsKey(userId) && userUsageAsMap.get(userId) != null) {
					dataMapList = (List<Map<String, Object>>) userUsageAsMap.get(userId);
				}
				addPerformanceMetrics(classId, lessonId, dataMapList, resultRow, collectionType, nextLevelType);
				userUsageAsMap.put(userId, dataMapList);
			}
			for (Map.Entry<String, Object> userUsageAsMapEntry : userUsageAsMap.entrySet()) {
				Map<String, Object> resultAsMap = new HashMap<>(2);
				resultAsMap.put(ApiConstants.USERUID, userUsageAsMapEntry.getKey());
				resultAsMap.put(ApiConstants.USAGE_DATA, userUsageAsMapEntry.getValue());
				dataMapAsList.add(resultAsMap);
			}
			responseParamDTO.setContent(dataMapAsList);
		}
		return responseParamDTO;
	}

	private ResponseParamDTO<Map<String, Object>> getPerformanceDataByLambda(String classId, String courseId,
			String unitId, String lessonId, String userUid, String collectionType, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		List<StudentsClassActivity> classActivityResultSetList = new ArrayList<>();
		ResultSet classActivityResultSet = cassandraService.getStudentsClassActivity(classId, courseId, unitId,
				lessonId, null);
		if (classActivityResultSet != null) {
			for (Row classActivityRow : classActivityResultSet) {
				StudentsClassActivity studentsClassActivity = new StudentsClassActivity();
				studentsClassActivity.setUnitId(classActivityRow.getString(ApiConstants._UNIT_UID));
				studentsClassActivity.setLessonId(classActivityRow.getString(ApiConstants._LESSON_UID));
				studentsClassActivity.setUserUid(classActivityRow.getString(ApiConstants._USER_UID));
				studentsClassActivity.setCollectionType(classActivityRow.getString(ApiConstants._COLLECTION_TYPE));
				studentsClassActivity.setScore(classActivityRow.getLong(ApiConstants.SCORE));
				studentsClassActivity.setReaction(classActivityRow.getLong(ApiConstants.REACTION));
				if (ApiConstants.COLLECTION.equals(studentsClassActivity.getCollectionType())) {
					studentsClassActivity.setCollectionId(classActivityRow.getString(ApiConstants._COLLECTION_UID));
					studentsClassActivity.setAssessmentId(classActivityRow.getString(ApiConstants._COLLECTION_UID));
					studentsClassActivity.setViews(classActivityRow.getLong(ApiConstants.VIEWS));
				} else if (ApiConstants.ASSESSMENT.equals(studentsClassActivity.getCollectionType())){
				  studentsClassActivity.setCollectionId(classActivityRow.getString(ApiConstants._COLLECTION_UID));
				  studentsClassActivity.setAssessmentId(classActivityRow.getString(ApiConstants._COLLECTION_UID));
					studentsClassActivity.setAttempts(classActivityRow.getLong(ApiConstants.VIEWS));
				}
				studentsClassActivity.setTimeSpent(classActivityRow.getLong(ApiConstants._TIME_SPENT));
				studentsClassActivity.setAttemptStatus(classActivityRow.getString(ApiConstants._ATTEMPT_STATUS));
				studentsClassActivity.setClassId(classActivityRow.getString(ApiConstants._CLASS_UID));
				classActivityResultSetList.add(studentsClassActivity);
			}
			List<StudentsClassActivity> filteredList = lambdaService
					.applyFiltersInStudentsClassActivity(classActivityResultSetList, collectionType,userUid);
			List<Map<String, Object>> result = new ArrayList<>();
      if (!collectionType.equalsIgnoreCase(ApiConstants.BOTH)) {
        List<Map<String, List<StudentsClassActivity>>> aggregatedList =
                lambdaService.aggregateStudentsClassActivityData(filteredList, collectionType, nextLevelType);
        for (Map<String, List<StudentsClassActivity>> aggregatedSubList : aggregatedList) {
          for (Map.Entry<String, List<StudentsClassActivity>> data : aggregatedSubList.entrySet()) {

            Map<String, Object> resultMap = new HashMap<>();
            if (data.getValue().size() == 0) {
              continue;
            }
            resultMap.put("userUid", data.getKey());
            resultMap.put("usageData", data.getValue());
            result.add(resultMap);
          }
        }
      } else {
        List<Map<String, List<Map<String, List<StudentsClassActivity>>>>> aggregatedList =
                lambdaService.aggregateStudentsClassActivityBothData(filteredList, collectionType, nextLevelType);
        for (Map<String, List<Map<String, List<StudentsClassActivity>>>> aggregatedSubList : aggregatedList) {
          for (Entry<String, List<Map<String, List<StudentsClassActivity>>>> data : aggregatedSubList.entrySet()) {
            Map<String, Object> resultMap = new HashMap<>();
            if (data.getValue().size() == 0) {
              continue;
            }
            resultMap.put("userUid", data.getKey());
            resultMap.put("usageData", data.getValue());
            result.add(resultMap);
          }
        }
      }
      responseParamDTO.setContent(result);
		}
		return responseParamDTO;
	}

	//TODO nextLevelType is hard coded temporarily. In future, store and get nextLevelType from CF
	private void addPerformanceMetrics(String classId, String lessonId, List<Map<String, Object>> dataMapList, Row resultRow, String collectionType, String nextLevelType) {
		Map<String, Object> dataAsMap = new HashMap<>(8);
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

	private ResponseParamDTO<Map<String, Object>> getAllStudentsPerformance(String classId, String courseId, String unitId, String lessonId, String gooruOid, String collectionType, String userUid) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		List<Map<String, Object>> resultDataAsList = new ArrayList<>();
		//Fetch student Latest session for the class
		Map<String, String> userSessions = getUsersLatestSessionId(classId, courseId, unitId, lessonId, gooruOid, userUid);
		for (Map.Entry<String, String> stringStringEntry : userSessions.entrySet()) {
			Map<String, Object> usageData = new HashMap<>(2);
			List<Map<String, Object>> sessionActivities = new ArrayList<>();
			usageData.put(ApiConstants.USER_UID, stringStringEntry.getValue());
			usageData.put(ApiConstants.USAGE_DATA, sessionActivities);
			// Fetch Usage Data
			if (StringUtils.isNotBlank(stringStringEntry.getKey())) {
				getResourceMetricsBySession(sessionActivities, stringStringEntry.getKey(), new HashMap<>(2));
			}
			resultDataAsList.add(usageData);
		}
		responseParamDTO.setContent(resultDataAsList);
		return responseParamDTO;
	}

	private List<Map<String, Object>> getPriorUsage(String sessionKey) {

		List<Map<String, Object>> sessionActivities = new ArrayList<>();
		ResultSet userSessionActivityResult = cassandraService.getUserSessionActivity(sessionKey);
		if (userSessionActivityResult != null) {
			for (Row userSessionActivityRow : userSessionActivityResult) {
				Map<String, Object> sessionActivityMetrics = new HashMap<>();
				String contentType = userSessionActivityRow.getString(ApiConstants._RESOURCE_TYPE);
				if(!ApiConstants.COLLECTION_OR_ASSESSMENT_PATTERN.matcher(contentType).matches()) {
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


		ResultSet userSessionActivityResult = cassandraService.getUserSessionActivity(sessionKey);
		if (userSessionActivityResult != null) {
			String itemName = ApiConstants.RESOURCES;
			for (Row userSessionActivityRow : userSessionActivityResult) {
				Map<String, Object> sessionActivityMetrics = new HashMap<>();
				String contentType = userSessionActivityRow.getString(ApiConstants._RESOURCE_TYPE);
				sessionActivityMetrics.put(ApiConstants.SESSIONID, userSessionActivityRow.getString(ApiConstants._SESSION_ID));
				sessionActivityMetrics.put(ApiConstants.GOORUOID, userSessionActivityRow.getString(ApiConstants._GOORU_OID));
				sessionActivityMetrics.put(ApiConstants.RESOURCE_TYPE, contentType);
				sessionActivityMetrics.put(ApiConstants.SCORE, userSessionActivityRow.getLong(ApiConstants.SCORE));
				sessionActivityMetrics.put(ApiConstants.TIMESPENT, userSessionActivityRow.getLong(ApiConstants._TIME_SPENT));
				sessionActivityMetrics.put(ApiConstants.VIEWS, userSessionActivityRow.getLong(ApiConstants.VIEWS));
				sessionActivityMetrics.put(ApiConstants.REACTION, userSessionActivityRow.getLong(ApiConstants.REACTION));
				sessionActivityMetrics.put(ApiConstants.EVENT_TIME, userSessionActivityRow.getLong(ApiConstants._EVENT_TIME));
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


		return Observable.<ResponseParamDTO<ContentTaxonomyActivity>> create(s -> {
			try {
					s.onNext(getUserDomainParentMasteryUsage(studentId, domainIds));
			} catch (Throwable t) {
				s.onError(t);
			}
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	private ResponseParamDTO<ContentTaxonomyActivity> getUserDomainParentMasteryUsage(String studentId, String domainIds) {

		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<>();

		if(StringUtils.isEmpty(domainIds)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E104, ApiConstants.COURSE_IDS);
		}
		ResultSet taxonomyParents = cassandraService.getTaxonomyParents(domainIds);
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


		return Observable.<ResponseParamDTO<ContentTaxonomyActivity>> create(s -> {
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
	}

	private ResponseParamDTO<ContentTaxonomyActivity> getSubjectActivity(String studentId, String subjectId) {

		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		ResultSet userSubjectActivityResult = cassandraService.getSubjectActivity(studentId, subjectId);
		Map<String, Set<String>> itemMap = new HashMap<>();
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

		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		ResultSet userCourseActivityResult = cassandraService.getCourseActivity(studentId, subjectId, courseId);
		Map<String, Set<String>> itemMap = new HashMap<>();
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

		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		ResultSet userDomainActivityResult = cassandraService.getDomainActivity(studentId, subjectId, courseId, domainId);
		Map<String, Set<String>> itemMap = new HashMap<>();
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

		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		ResultSet userDomainActivityResult = cassandraService.getDomainActivity(studentId, subjectId, courseId, domainId);
		Map<String, Set<String>> itemMap = new HashMap<>();
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

		ResponseParamDTO<ContentTaxonomyActivity> responseParamDTO = new ResponseParamDTO<>();
		List<ContentTaxonomyActivity> contentTaxonomyActivityList = new ArrayList<>();
		ResultSet userStandardActivityResult = cassandraService
			.getStandardsActivity(studentId, subjectId, courseId, domainId, standardsId);
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


		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(fetchTeacherGrade(teacherUid, userUid, sessionId));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	private ResponseParamDTO<Map<String, Object>> fetchTeacherGrade(String teacherUid, String userUid, String sessionId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		if(StringUtils.isBlank(teacherUid)) {
			return responseParamDTO;
		}
		ResultSet result = cassandraService.getStudentQuestionGrade(teacherUid, userUid, sessionId);
		List<Map<String, Object>> teacherGradeAsList = new ArrayList<>();
		if(result != null) {
			for(Row row : result) {
				Map<String, Object> teacherGradeAsMap = new HashMap<>();
				teacherGradeAsMap.put(ApiConstants.QUESTION_ID, row.getString(ApiConstants._QUESTION_ID));
				teacherGradeAsMap.put(ApiConstants.SCORE, row.getLong(ApiConstants.SCORE));
				teacherGradeAsList.add(teacherGradeAsMap);
			}
			responseParamDTO.setContent(teacherGradeAsList);
		}
		return responseParamDTO;
	}

	public Observable<ResponseParamDTO<SessionTaxonomyActivity>> getResourceUsage(String sessionId, String resourceIds) {


		return Observable.<ResponseParamDTO<SessionTaxonomyActivity>> create(s -> {
			s.onNext(fetchResourceUsage(sessionId, resourceIds));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	@Override
	public Observable<ResponseParamDTO<Map<String, Object>>> getStatisticalMetrics(String gooruOids) {

		return Observable.<ResponseParamDTO<Map<String, Object>>> create(s -> {
			s.onNext(getStatMetrics(gooruOids));
			s.onCompleted();
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	private ResponseParamDTO<Map<String, Object>> getStatMetrics(String gooruOids) {
		if (StringUtils.isBlank(gooruOids)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E104, ApiConstants.RESOURCE_IDS);
		}
		ResponseParamDTO<Map<String, Object>> resourceUsageObject = new ResponseParamDTO<>();
		List<Map<String, Object>> resourceUsageList = new ArrayList<>();
		for (String resourceId : gooruOids.split(ApiConstants.COMMA)) {
			ResultSet statMetricsResult = cassandraService.getStatisticalMetrics(resourceId);
			if (statMetricsResult != null) {
				String clusterKey = null;
				Map<String, Object> resourceUsage = null;
				for (Row statMetricsRow : statMetricsResult.all()) {
					String gooruOid = statMetricsRow.getString(ApiConstants._CLUSTERING_KEY);
					if (clusterKey == null || (clusterKey != null && !clusterKey.equalsIgnoreCase(gooruOid))) {
						resourceUsage = new HashMap<>();
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

	private ResponseParamDTO<SessionTaxonomyActivity> fetchResourceUsage(String userUid, String resourceIds) {

		if(StringUtils.isBlank(resourceIds)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E104, ApiConstants.RESOURCE_IDS);
		}
		ResponseParamDTO<SessionTaxonomyActivity> resourceUsageObject = new ResponseParamDTO<>();
		List<SessionTaxonomyActivity> sessionTaxonomyActivities = new ArrayList<>();
		for(String resourceId : resourceIds.split(ApiConstants.COMMA)) {
			ResultSet sessionResourceActivityResult = cassandraService
				.getSessionResourceTaxonomyActivity(ApiConstants.AS_TILDA+userUid, resourceId.trim());
			if(sessionResourceActivityResult != null) {
				for(Row userSessionColumn : sessionResourceActivityResult) {
					SessionTaxonomyActivity sessionTaxonomyActivity = new SessionTaxonomyActivity();
					sessionTaxonomyActivity.setResourceId(userSessionColumn.getString(ApiConstants._GOORU_OID));
					sessionTaxonomyActivity.setViews(userSessionColumn.getLong(ApiConstants.VIEWS));
					sessionTaxonomyActivity.setTimespent(userSessionColumn.getLong(ApiConstants._TIME_SPENT));
					sessionTaxonomyActivity.setSubjectIds(userSessionColumn.getString(ApiConstants._SUBJECT_ID));
					sessionTaxonomyActivity.setCourseIds(userSessionColumn.getString(ApiConstants._COURSE_ID));
					sessionTaxonomyActivity.setDomainIds(userSessionColumn.getString(ApiConstants._DOMAIN_ID));
					sessionTaxonomyActivity.setStandardsIds(userSessionColumn.getString(ApiConstants._STANDARDS_ID));
					sessionTaxonomyActivity.setDisplayCode(userSessionColumn.getString(ApiConstants._DISPLAY_CODE));
					sessionTaxonomyActivity.setLearningTargetsIds(userSessionColumn.getString(ApiConstants._LEARNING_TARGETS_ID));
					sessionTaxonomyActivities.add(sessionTaxonomyActivity);
				}
			}
		}
		List<SessionTaxonomyActivity> result = lambdaService.aggregateSessionTaxonomyActivityByGooruOid(sessionTaxonomyActivities);
		resourceUsageObject.setContent(result);
		return resourceUsageObject;
	}

	private ResponseParamDTO<Map<String, Object>> getStudentCurrentLocation(String userUid, String classId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<>();
		ResultSet resultRows = cassandraService.getUserCurrentLocationInClass(classId, userUid);
		if (resultRows != null) {
			for(Row resultColumns : resultRows){
			Map<String, Object> dataAsMap = new HashMap<>();
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
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<>();
		List<UserContentLocation> userContentLocationObject = new ArrayList<>();
		ResultSet resultRows = cassandraService.getAllUserCurrentLocationInClass(classId);
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
			Map<String, Long> peers = userContentLocationObject.stream().collect(Collectors.groupingBy(object -> getGroupByField(object, nextLevelType), Collectors.counting()));
			for (Map.Entry<String, Long> peer : peers.entrySet()) {
				Map<String, Object> dataAsMap = new HashMap<>();
				dataAsMap.put(ApiConstants.getResponseNameByType(nextLevelType), peer.getKey());
				dataAsMap.put(ApiConstants.PEER_COUNT, peer.getValue());
				dataMapAsList.add(dataAsMap);
			}
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}

	private ResponseParamDTO<Map<String, Object>> getUserPeerData(String classId, String courseId, String unitId, String lessonId, String nextLevelType) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<>();
		ResultSet resultRows = cassandraService.getAllUserCurrentLocationInClass(classId);
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
					Map<String, Object> dataAsMap = new HashMap<>(3);
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
				Set<String> childItems = new HashSet<>();
				childItems.add(childKey);
				itemMap.put(parentKey, childItems);
			}
		}
	}

	private void includeAdditionalMetrics(List<ContentTaxonomyActivity> taxonomyActivities, Map<String,Set<String>> attemptedItemMap, Integer depth) {

		Map<String, Long> itemCount = new HashMap<>();
		if(attemptedItemMap != null ){
			Set<String> childIds = attemptedItemMap.keySet();
			ResultSet childItemsCount = cassandraService.getTaxonomyItemCount(childIds);
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
			return (long) attemptedItemMap.get(id).size();
		} else {
			return 0L;
		}
	}

	private void calculateScoreInPercentage(ContentTaxonomyActivity contentTaxonomyActivity) {

		if(contentTaxonomyActivity.getScore() != null && contentTaxonomyActivity.getAttempts() != null) {
			contentTaxonomyActivity.setScoreInPercentage(
				(long) Math.round((contentTaxonomyActivity.getScore() / contentTaxonomyActivity.getAttempts())));
			//Avoiding score in response
			contentTaxonomyActivity.setScore(null);
		}
	}

	private Map<String, String> getUsersLatestSessionId(String classId, String courseId, String unitId, String lessonId, String collectionId, String userId) {

		Map<String,String> usersSession = new HashMap<>();
		ResultSet  usersLatestSession;
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

	public long getCulCollectionCount(String classId, String leafNodeId,
			String collectionType) {
		long totalCount;
		long collectionCount = 0L;
		long externalAssCount = 0L;
		long assessmentCount = 0L;
		ResultSet columnList = cassandraService.getClassCollectionCount(classId, leafNodeId);
		if (columnList != null) {
			for (Row columns : columnList) {
				String contentType = columns.getString("content_type");
				long count = columns.getLong("total_count");
				if (contentType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
					collectionCount = count;
				} else if (contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) {
					assessmentCount = count;
				} else {
					externalAssCount = count;
				}
			}
		}
		if(ApiConstants.COLLECTION.equals(collectionType)){
		  totalCount = collectionCount;
		}else if(ApiConstants.ASSESSMENT.equals(collectionType)){
		  totalCount = assessmentCount + externalAssCount;
		}else{
		  totalCount = collectionCount + assessmentCount + externalAssCount;
		}
		return totalCount;
	}

	@Override
	public Observable<ResponseParamDTO<SessionTaxonomyActivity>> getSessionTaxonomyActivity(String sessionId, String levelType) {


		return Observable.<ResponseParamDTO<SessionTaxonomyActivity>> create(s -> {
			try {
			s.onNext(fetchSessionTaxonomyActivity(sessionId, levelType));
			s.onCompleted();
			}catch(Exception e) {
				s.onError(e);
			}
		}).subscribeOn(Schedulers.from(observableExecutor));
	}

	@Override
	public Observable<ResponseParamDTO<Map<String,Object>>> getEvent(String eventId) {
		return Observable.<ResponseParamDTO<Map<String,Object>>> create(s -> {
			try {
			s.onNext(fetchEvent(eventId));
			s.onCompleted();
			}catch(Exception e) {
				s.onError(e);
			}
		}).subscribeOn(Schedulers.from(observableExecutor));
	}
	private ResponseParamDTO<Map<String,Object>> fetchEvent(String eventId){
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<>();
		ResultSet result = cassandraService.getEvent(eventId);
		List<Map<String,Object>> content = new ArrayList<>();
		if(result != null) {
			for(Row row : result) {
				Map<String,Object> eventMap = new HashMap<>();
				eventMap.put(ApiConstants._EVENT_ID, row.getString(ApiConstants._EVENT_ID));
				eventMap.put("fields", row.getString("fields"));
				content.add(eventMap);
			}
			responseParamDTO.setContent(content);
		}
		return responseParamDTO;
	}
	private ResponseParamDTO<SessionTaxonomyActivity> fetchSessionTaxonomyActivity(String sessionId, String levelType) {


		ResultSet result = cassandraService.getSessionResourceTaxonomyActivity(sessionId, null);
		ResponseParamDTO<SessionTaxonomyActivity> responseParamDTO = new ResponseParamDTO<>();
		List<SessionTaxonomyActivity> sessionTaxonomyActivities = new ArrayList<>();
		if(result != null) {
			for(Row row : result) {
				if(!ApiConstants.QUESTION.equalsIgnoreCase(row.getString(ApiConstants._RESOURCE_TYPE))) {
					continue;
				}
				SessionTaxonomyActivity sessionTaxonomyActivity = new SessionTaxonomyActivity();
				sessionTaxonomyActivity.setQuestionId(row.getString(ApiConstants._GOORU_OID));
				sessionTaxonomyActivity.setSubjectId(row.getString(ApiConstants._SUBJECT_ID));
				sessionTaxonomyActivity.setCourseId(row.getString(ApiConstants._COURSE_ID));
				sessionTaxonomyActivity.setDomainId(row.getString(ApiConstants._DOMAIN_ID));
				sessionTaxonomyActivity.setStandardsId(row.getString(ApiConstants._STANDARDS_ID));
				sessionTaxonomyActivity.setLearningTargetsId(row.getString(ApiConstants._LEARNING_TARGETS_ID));
				sessionTaxonomyActivity.setAnswerStatus(row.getString(ApiConstants.ANSWER_STATUS));
				sessionTaxonomyActivity.setQuestionType(row.getString(ApiConstants._QUESTION_TYPE));
				sessionTaxonomyActivity.setDisplayCode(row.getString(ApiConstants._DISPLAY_CODE));
				sessionTaxonomyActivity.setReaction(row.getLong(ApiConstants.REACTION));
				sessionTaxonomyActivity.setScore(row.getLong(ApiConstants.SCORE));
				sessionTaxonomyActivity.setAttempts(row.getLong(ApiConstants.VIEWS));
				sessionTaxonomyActivity.setTimespent(row.getLong(ApiConstants._TIME_SPENT));
				sessionTaxonomyActivities.add(sessionTaxonomyActivity);
			}
		}
		responseParamDTO.setContent(lambdaService.aggregateSessionTaxonomyActivity(sessionTaxonomyActivities, levelType));
		return responseParamDTO;
	}
}
