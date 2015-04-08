package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;
import org.json.JSONObject;

public interface ConfigurationService {

	ResponseParamDTO<Map<String, Object>> addFormula(String traceId, String sessionToken,String eventName,String aggregateType,JSONObject formulaJSON);

	ResponseParamDTO<Map<String, String>> listFormula(String traceId, String sessionToken,String eventName);
	
	ResponseParamDTO<Map<String, Object>> addSettings(String traceId, String cfName,String keyName, String data) throws Exception ;
	
	ResponseParamDTO<Map<String, Object>> addCounterSettings(String traceId, String cfName,String keyName,String data) throws Exception;
	
	ResponseParamDTO<Map<String, Object>> viewSettings(String traceId, String cfName,String keyName);
	
	ResponseParamDTO<Map<String, String>> migrateCFData(String traceId, String sourceCF,String targetCF,String sourceKey,String targetKey);
}
