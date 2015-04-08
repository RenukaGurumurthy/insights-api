package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface UserService {

	ResponseParamDTO<Map<String, Object>> getPreferenceDataByType(String traceId, String userUid, String data) throws Exception;
	
	ResponseParamDTO<Map<String, Object>> getUserData(String traceId, String userUId) throws Exception;
	
	ResponseParamDTO<Map<String, Object>> getTopPreferenceList(String traceId) throws Exception;
	
	ResponseParamDTO<Map<Object, Object>> getProficiencyData(String traceId, String userUid, String data) throws Exception;

	ResponseParamDTO<Map<String, Object>> getTopProficiencyList(String traceId) throws Exception;

}
