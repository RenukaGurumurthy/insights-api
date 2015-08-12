package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;
import org.json.JSONObject;

public interface ConfigurationService {

	ResponseParamDTO<Map<String, Object>> addFormula(String sessionToken,String eventName,String aggregateType,JSONObject formulaJSON);

	ResponseParamDTO<Map<String, String>> listFormula(String sessionToken,String eventName);
	
	ResponseParamDTO<Map<String, Object>> addSettings(String cfName,String keyName, String data) throws Exception ;
	
	ResponseParamDTO<Map<String, Object>> addCounterSettings(String cfName,String keyName,String data) throws Exception;
	
	ResponseParamDTO<Map<String, Object>> viewSettings(String cfName,String keyName);
	
	ResponseParamDTO<Map<String, String>> migrateCFData(String sourceCF,String targetCF,String sourceKey,String targetKey);
}
