package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface ClassService {

	ResponseParamDTO<Map<String,Object>> getCourseUsage(String traceId, String classId, String courseId, String userUid, Boolean getUsageData, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getUnitUsage(String traceId, String classId, String courseId, String unitId, String userUid,String collectionType, Boolean getUsageData, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getLessonUsage(String traceId, String classId, String courseId, String unitId, String lessonId, String userUid, Boolean getUsageData, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getCoursePlan(String traceId, String classId, String courseId, String userUid, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getUnitPlan(String traceId, String classId, String courseId, String unitId, String userUid, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getLessonPlan(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentIds, String userUid, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getCourseProgress(String traceId, String classId, String courseId,String collectionType, String userUid, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getUnitProgress(String traceId, String classId, String courseId, String unitId, String userUid, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String, Object>> getUserSessions(String traceId, String classId, String courseId, String unitId, String lessonId, String collectionId, String collectionType, String userUid,
			boolean openSession,boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getStudentAssessmentData(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentId, String userUid, boolean isSecure) throws Exception;

}
