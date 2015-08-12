package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.type.TypeReference;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.spring.exception.BadRequestException;
import org.gooru.insights.api.spring.exception.InsightsServerException;
import org.gooru.insights.api.utils.JsonDeserializer;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

@Service
public class LiveDashboardServiceImpl implements LiveDashboardService {

	@Autowired
	private BaseService baseService;

	@Autowired
	private CassandraService cassandraService;
	
	public ResponseParamDTO<Map<String, Object>> addSettings(String cfName, String keyName, String data) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		JSONObject jsonData = getBaseService().validateJSON(data);
		Map<String, Object> resultMap = new HashMap<String, Object>();
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		Map<String, Object> m = new JsonDeserializer().deserializeTypeRef(jsonData.toString(), new TypeReference<Map<String, Object>>() {
		});
		getCassandraService().addRowKeyValues(cfName, keyName, m);
		resultMap.put("status", "Successfully added!!");
		resultList.add(resultMap);
		responseParamDTO.setContent(resultList);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String, Object>> addCounterSettings(String cfName, String keyName, String data) throws Exception {

		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, Object> resultMap = new HashMap<String, Object>();
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (cfName.isEmpty() || keyName.isEmpty()) {
			throw new BadRequestException("Mandatory fields missing");
		}
		JSONObject dataObj = getBaseService().validateJSON(data);

		Map<String, Object> m = new JsonDeserializer().deserializeTypeRef(dataObj.toString(), new TypeReference<Map<String, Object>>() {
		});
		getCassandraService().addCounterRowKeyValues(cfName, keyName, m);
		resultMap.put("status", "Successfully added.");
		resultList.add(resultMap);
		responseParamDTO.setContent(resultList);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String, Object>> viewSettings(String cfName, String keyName) {
		
		try {
			ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
			List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
			OperationResult<ColumnList<String>> settingsMap = getCassandraService().read(cfName, keyName);
			for (Column<String> detail : settingsMap.getResult()) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
				resultMap.put(detail.getName(), detail.getStringValue());
				resultList.add(resultMap);
			}
			responseParamDTO.setContent(resultList);
			return responseParamDTO;
		} catch (Exception e) {
			throw new InsightsServerException(e.getMessage());
		}
	}
	
	private BaseService getBaseService() {
		return baseService;
	}
	
	private CassandraService getCassandraService() {
		return cassandraService;
	}
}
