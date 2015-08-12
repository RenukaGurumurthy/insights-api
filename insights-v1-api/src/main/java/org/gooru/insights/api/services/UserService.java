package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface UserService {

	ResponseParamDTO<Map<String, Object>> getPreferenceDataByType(String userUid, String data) throws Exception;
	
	ResponseParamDTO<Map<String, Object>> getUserData(String userUId) throws Exception;
	
	ResponseParamDTO<Map<String, Object>> getTopPreferenceList() throws Exception;
	
	ResponseParamDTO<Map<Object, Object>> getProficiencyData(String userUid, String data) throws Exception;

	ResponseParamDTO<Map<String, Object>> getTopProficiencyList() throws Exception;

}
