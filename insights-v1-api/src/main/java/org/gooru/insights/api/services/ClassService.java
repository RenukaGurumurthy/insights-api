package org.gooru.insights.api.services;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface ClassService {

	ResponseParamDTO<Map<String,Object>> getAllStudentsUnitUsage(String traceId, String classId, String courseId, String unitId, String userUid,String collectionType, Boolean getUsageData, boolean isSecure) throws Exception;
		
	ResponseParamDTO<Map<String,Object>> getCoursePlan(String traceId, String classId, String courseId, String userUid, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getUnitPlan(String traceId, String classId, String courseId, String unitId, String userUid, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getLessonAssessmentsUsage(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentIds, String userUid, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getCourseProgress(String traceId, String classId, String courseId, String userUid, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getUnitProgress(String traceId, String classId, String courseId, String unitId, String userUid, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String, Object>> getUserSessions(String traceId, String classId, String courseId, String unitId, String lessonId, String collectionId, String collectionType, String userUid,
			boolean openSession,boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getStudentAssessmentData(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid, String collectionType, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getStudentAssessmentSummary(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentId, String userUid, String sessionId, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String, Object>> getSessionStatus(String traceId, String sessionId, String contentGooruId, String collectionType, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getStudentsCollectionData(String traceId, String classId, String courseId, String unitId, String lessonId, String collectionId, boolean isSecure) throws Exception;

	List<Map<String,Object>> getAssociatedItems(String traceId,String rowKey,String type,boolean fetchMetaData,Collection<String> columns,Map<String,String> aliesName);

	List<Map<String,Object>> getStudents(String traceId, String classId);

	List<String> getSessions(String traceId, Collection<String> rowKeys);

	List<Map<String,Object>> getIdSeparatedMetrics(String traceId, Collection<String> rowKeys,String columnFamily, Collection<String> columns, String userIds,boolean isUserIdInKey,String collectionIds, boolean userProcess);

	ResponseParamDTO<Map<String, Object>> findUsageAvailable(String traceId, String classGooruId ,String courseGooruId,String unitGooruId,String lessonGooruId,String contentGooruId) throws Exception;

	List<Map<String,Object>> getResourcesMetaData(String traceId, Collection<String> keys,Collection<String> resourceColumns,String type,Map<String,String> aliesNames);

	void getResourceMetaData(Map<String, Object> dataMap, String traceId,String type, String key,Map<String,String> aliesNames);
}


