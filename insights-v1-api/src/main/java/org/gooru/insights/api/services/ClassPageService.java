package org.gooru.insights.api.services;

import java.util.Map;

import javassist.NotFoundException;

import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface ClassPageService {

	ResponseParamDTO<Map<String,Object>> getClasspageCollectionUsage(String collectionId,String data,boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getClasspageResourceUsage(String collectionId,String data,boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getClasspageUserUsage(String collectionId,String data,boolean isSecure) throws Exception;

	ResponseParamDTO<Map<String,Object>> getClasspageUsers(String classId,String data) throws Exception;

	ResponseParamDTO<Map<String,Object>> getClasspageResourceOEtext(String collectionId,String data) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getUserSessions(String data, String collectionId) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getClasspageGrade(String classId,String data,boolean isSecure) throws Exception;
	
	ResponseParamDTO<Map<String,Object>> getResourceInfo(String resouceId, String data);
	
	ResponseParamDTO<Map<String,Object>> getClassProgress(String classId,String data) throws NotFoundException, Exception;
	
	ResponseParamDTO<Map<String,Object>> getExportReport(String format,String classId,String reportType,String data,String timeZone,HttpServletResponse response) throws Exception;
	
}
