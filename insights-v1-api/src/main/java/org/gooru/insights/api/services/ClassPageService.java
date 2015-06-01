package org.gooru.insights.api.services;

import java.util.Map;

import javassist.NotFoundException;

import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface ClassPageService {

	ResponseParamDTO<Map<String,Object>> getClasspageCollectionUsage(String traceId, String collectionId,String data,boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getClasspageResourceUsage(String traceId, String collectionId,String data,boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getClasspageUserUsage(String traceId, String collectionId,String data,boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getClasspageUsers(String traceId, String classId,String data) throws Exception;

	ResponseParamDTO<Map<String,Object>> getClasspageResourceOEtext(String traceId, String collectionId,String data) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getUserSessions(String traceId, String data, String collectionId) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getClasspageGrade(String traceId, String classId,String data,boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getResourceInfo(String traceId, String resouceId, String data);
	
	ResponseParamDTO<Map<String,Object>> getClassProgress(String traceId, String classId,String data) throws NotFoundException, Exception;
	
	ResponseParamDTO<Map<String,Object>> getExportReport(String traceId, String format,String classId,String reportType,String data,String timeZone,HttpServletResponse response) throws Exception;
	
}
