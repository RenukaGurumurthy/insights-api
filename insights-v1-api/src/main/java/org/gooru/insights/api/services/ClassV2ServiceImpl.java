package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.constants.InsightsConstant;
import org.gooru.insights.api.constants.ApiConstants.SessionAttributes;
import org.gooru.insights.api.constants.InsightsConstant.ColumnFamily;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.utils.ServiceUtils;
import org.gooru.insights.api.utils.ValidationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class ClassV2ServiceImpl implements ClassV2Service, InsightsConstant{
	
	@Autowired
	private CassandraV2Service cassandraService;

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
		// TODO Enabled for class varification
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
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		ColumnList<String> resultColumns = getCassandraService().getUserCurrentLocation(ColumnFamily.STUDENT_LOCATION.getColumnFamily(), userUid, classId);
		if (resultColumns != null && resultColumns.size() > 0) {
			Map<String, Object> dataAsMap = new HashMap<String, Object>();
			dataAsMap.put(ApiConstants.CLASS_GOORU_ID, resultColumns.getStringValue("class_uid", null));
			dataAsMap.put("courseId", resultColumns.getStringValue("course_uid", null));
			dataAsMap.put("unitId", resultColumns.getStringValue("unit_uid", null));
			dataAsMap.put("lessonId", resultColumns.getStringValue("lesson_uid", null));
			dataAsMap.put("gooruOid", resultColumns.getStringValue("collection_uid", null));
			dataMapAsList.add(dataAsMap);
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getUserPeers(String classId, String courseId, String unitId, String lessonId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		String rowKey = getBaseService().appendTilda(classId, courseId, unitId, lessonId);
		Rows<String, String> resultRows = getCassandraService().readColumnsWithKey(ColumnFamily.CLASS_ACTIVITY_PEER_COUNTS.getColumnFamily(), rowKey);
		if (resultRows != null && resultRows.size() > 0) {
			for(Row<String, String> resultRow : resultRows) {
				Map<String, Object> dataAsMap = new HashMap<String, Object>();
				ColumnList<String> columnList = resultRow.getColumns();
				dataAsMap.put(ApiConstants.GOORUOID, columnList.getStringValue(ApiConstants._LEAF_GOORU_OID, null));
				dataAsMap.put(ApiConstants.ACTIVE_PEER_COUNT, columnList.getLongValue(ApiConstants._ACTIVE_GOORU_OID, 0L));
				dataAsMap.put(ApiConstants.LEFT_PEER_COUNT, columnList.getLongValue(ApiConstants._LEFT_PEER_COUNT, 0L));
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
		String sessionKey = null;
		//TODO validate ClassId
		//isValidClass(classId);
		if ((sessionId != null && StringUtils.isNotBlank(sessionId.trim()))) {
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
		
		//Fetch score and evidence of assessment
		if (StringUtils.isNotBlank(sessionKey)) {
			CqlResult<String, String> userSessionActivity = getCassandraService().readWithCondition(ColumnFamily.USER_SESSION_ACTIVITY.getColumnFamily(), new String[][]{{ApiConstants._SESSION_ID, sessionKey}});
			List<Map<String,Object>> sessionActivities = new ArrayList<Map<String,Object>>();
			if(userSessionActivity != null && userSessionActivity.hasRows()) {
				Rows<String,String> sessionActivties = userSessionActivity.getRows();
				for(Row<String,String> activities : sessionActivties) {
					Map<String,Object> sessionMetrics = new HashMap<String,Object>();
					ColumnList<String> sessionColumns = activities.getColumns();
					sessionMetrics.put(ApiConstants.SESSIONID, sessionColumns.getStringValue(ApiConstants._SESSION_ID, null));
					sessionMetrics.put(ApiConstants.GOORUOID, sessionColumns.getStringValue(ApiConstants._GOORU_OID, null));
					sessionMetrics.put(ApiConstants.RESOURCE_TYPE, sessionColumns.getStringValue(ApiConstants._RESOURCE_TYPE, null));
					sessionMetrics.put(ApiConstants.SCORE, sessionColumns.getLongValue(ApiConstants.SCORE, null));
					sessionMetrics.put(ApiConstants.VIEWS, sessionColumns.getLongValue(ApiConstants.VIEWS, null));
					sessionMetrics.put(ApiConstants.TIMESPENT, sessionColumns.getLongValue(ApiConstants._TIME_SPENT, null));
					if(sessionColumns.getStringValue(ApiConstants._RESOURCE_TYPE, ApiConstants.STRING_EMPTY).equalsIgnoreCase(ApiConstants.COLLECTION)) {
						usageData.put(ApiConstants.COLLECTION, sessionMetrics);
					} else {
						sessionMetrics.put(ApiConstants.COLLECTION_ITEM_ID, sessionColumns.getStringValue(ApiConstants.COLLECTIONITEMID, null));
						sessionMetrics.put(ApiConstants.RESOURCE_FORMAT, sessionColumns.getStringValue(ApiConstants._RESOURCE_FORMAT, null));
						sessionMetrics.put(ApiConstants.ATTEMPTS, sessionColumns.getLongValue(ApiConstants.ATTEMPTS, null));
						sessionMetrics.put(ApiConstants.REACTION, sessionColumns.getLongValue(ApiConstants.REACTION, null));
						sessionMetrics.put(ApiConstants.ANSWER_OBJECT, sessionColumns.getStringValue(ApiConstants._ANSWER_OBJECT, null));
						sessionActivities.add(sessionMetrics);
					}
				}
				usageData.put(ApiConstants.RESOURCES, sessionActivities);
			}
			
		}
		summaryData.add(usageData);
		responseParamDTO.setContent(summaryData);
		return responseParamDTO;
	}
}
