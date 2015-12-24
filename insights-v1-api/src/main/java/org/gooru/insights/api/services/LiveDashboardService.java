package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface LiveDashboardService {

	ResponseParamDTO<Map<String, Object>> addSettings(String cfName,String keyName, String data) throws Exception;
	
	ResponseParamDTO<Map<String, Object>> addCounterSettings(String cfName,String keyName,String data) throws Exception;
	
	ResponseParamDTO<Map<String, Object>> viewSettings(String cfName,String keyName);
}
