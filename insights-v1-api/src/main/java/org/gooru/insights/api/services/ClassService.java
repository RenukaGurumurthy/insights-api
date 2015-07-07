package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface ClassService {

	ResponseParamDTO<Map<String,Object>> getCourseUsage(String traceId, String classId, String courseId, String userUid, Boolean getUsageData, boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getUnitUsage(String traceId, String classId, String courseId, String unitId, String userUid, Boolean getUsageData, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getLessonUsage(String traceId, String classId, String courseId, String unitId, String lessonId, String userUid, Boolean getUsageData, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getCoursePlanView(String traceId, String classId, String courseId, String userUid, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getUnitPlanView(String traceId, String classId, String courseId, String unitId, String userUid, boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getItemsUsage(String traceId, String baseKey, String userUid, boolean isUsageRequired,boolean isMetaRequired, boolean isSecure,String parentGooruIds) throws Exception;


}
