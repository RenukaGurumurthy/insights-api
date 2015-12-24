package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jackson.type.TypeReference;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.spring.exception.BadRequestException;
import org.gooru.insights.api.spring.exception.InsightsServerException;
import org.gooru.insights.api.utils.JsonDeserializer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

@Service
public class ConfigurationServiceImpl implements ConfigurationService, InsightsConstant {

	@Autowired
	private CassandraService cassandraService;

	@Autowired
	private BaseService baseService;

	public ResponseParamDTO<Map<String,Object>> addFormula(String sessionToken, String eventName, String aggregateType, JSONObject formulasJson) {

		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		List<Map<String, Object>> resultList = new ArrayList<Map<String,Object>>();
		if (formulasJson.has(formulaDetail.FORMULAS.getName()) && formulasJson.has(formulaDetail.EVENTS.getName())) {
			try {
				JSONArray jsonArray = new JSONArray();
				jsonArray = formulasJson.getJSONArray(formulaDetail.FORMULAS.getName());

				Map<Integer, String> idMap = new ConcurrentHashMap<Integer, String>();
				for (int i = 0; i < jsonArray.length(); i++) {
					JSONObject formulaJson = jsonArray.getJSONObject(i);
					if (formulaJson.has(formulaDetail.ID.getName()) && formulaJson.get(formulaDetail.ID.getName()) != null && formulaJson.has(formulaDetail.FORMULA.getName())
							&& formulaJson.has(formulaDetail.NAME.getName()) && formulaJson.has(formulaDetail.REQUESTVALUES.getName()) && formulaJson.has(formulaDetail.STATUS.getName())) {

						boolean hasRequestValues = true;
						String[] requestValues = formulaJson.get(formulaDetail.REQUESTVALUES.getName()).toString().split(",");
						for (String requestValue : requestValues) {
							if (!formulaJson.has(requestValue)) {
								hasRequestValues = false;
							}
						}
						if (!hasRequestValues) {
							throw new BadRequestException("requestValues should have their own values");
						}
						idMap.put(Integer.valueOf(formulaJson.get(formulaDetail.ID.getName()).toString()), formulaJson.toString());
					} else {
						throw new BadRequestException("Missed mandatory values and its should not be NULL");
					}
				}
				Map<String, String> formulaDetails = baseService.getStringValue(cassandraService.read("formula_detail", eventName));
				try {
					JSONObject json;
					JSONArray fetchedJsonArray = new JSONArray();
					boolean hasdata = false;
					if (getBaseService().notNull(formulaDetails)) {
						json = new JSONObject(formulaDetails.get(formulaDetail.FORMULA.getName()));
						fetchedJsonArray = json.getJSONArray(formulaDetail.FORMULAS.getName());
						for (int i = 0; i < fetchedJsonArray.length(); i++) {
							json = fetchedJsonArray.getJSONObject(i);
							if (json.has(formulaDetail.ID.getName()) && idMap.containsKey(json.get(formulaDetail.ID.getName()))) {
								fetchedJsonArray.put(i, new JSONObject(idMap.get(json.get(formulaDetail.ID.getName()))));
								idMap.remove(idMap.get(json.get(formulaDetail.ID.getName())));
								hasdata = true;
							}
						}
					}
					if (!hasdata) {
						for (Map.Entry<Integer, String> entry : idMap.entrySet()) {
							JSONObject rawJson = new JSONObject(entry.getValue());
							rawJson.put(formulaDetail.CREATEDON.getName(), System.currentTimeMillis());
							fetchedJsonArray.put(rawJson);
						}
					}
					json = new JSONObject();
					json.put(formulaDetail.EVENTS.getName(), formulasJson.get(formulaDetail.EVENTS.getName()));
					json.put(formulaDetail.FORMULAS.getName(), fetchedJsonArray);
					formulaDetails = new HashMap<String, String>();
					formulaDetails.put(formulaDetail.FORMULA.getName(), json.toString());
					formulaDetails.put(formulaDetail.AGGREGATETYPE.getName(), aggregateType != null ? aggregateType : formulaDetail.DEFAULT_AGGREGATETYPE.getName());
					getCassandraService().putStringValue(ColumnFamily.FORMULA_DETAIL.getColumnFamily(), eventName, formulaDetails);
				} catch (JSONException e) {
					throw new InsightsServerException("OOP's something went wrong !!! Try after some time" + e);
				}
 
			} catch (JSONException e) {
				throw new BadRequestException("formula parameter should have a json Array");
			}
			Map<String,Object> resultMap = new HashMap<String, Object>();
				resultMap.put(STATUS, CREATED);
				resultMap.put(EVENT_NAME, eventName);
			resultList.add(resultMap);
			responseParamDTO.setContent(resultList);
			return responseParamDTO;
		} else {
			throw new BadRequestException("Missed mandatory values and its should not be NULL");
		}
	}

	public ResponseParamDTO<Map<String,String>> listFormula(String sessionToken,String eventName) {
		ResponseParamDTO<Map<String,String>> responseParamDTO = new ResponseParamDTO<Map<String,String>>();
		List<Map<String,String>> resultList = new ArrayList<Map<String,String>>();
			List<String> columnList = new ArrayList<String>();
			columnList.add(formulaDetail.FORMULA.getName());
			Map<String, String> resultMap = getBaseService().getStringValue(getCassandraService().read(ColumnFamily.FORMULA_DETAIL.getColumnFamily(), eventName, columnList));
			for (Map.Entry<String, String> entry : resultMap.entrySet()) {
				if (formulaDetail.FORMULA.getName().equalsIgnoreCase(entry.getKey())) {
					resultMap = new HashMap<String, String>();
					resultMap.put("formula",entry.getValue());
					resultList.add(resultMap);
					responseParamDTO.setContent(resultList);
					return responseParamDTO;
				}
			}
			return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String, Object>> addSettings(String cfName, String keyName, String data) throws Exception {

		JSONObject dataObj = getBaseService().validateJSON(data);
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		Map<String, Object> dataMap = new JsonDeserializer().deserializeTypeRef(dataObj.toString(), new TypeReference<Map<String, Object>>() {
		});
		if (cfName == null) {
			cfName = ColumnFamily.CONFIG_SETTING.getColumnFamily();
		}
		getCassandraService().addRowKeyValues(cfName, keyName, dataMap);
		dataMap = new HashMap<String, Object>();
		dataMap.put("status", "Successfully added!!");
		resultList.add(dataMap);
		responseParamDTO.setContent(resultList);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String, Object>> addCounterSettings(String cfName,String keyName,String data) throws Exception {
		
		JSONObject dataObj = getBaseService().validateJSON(data);
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		Map<String,Object> dataMap = new JsonDeserializer().deserializeTypeRef(dataObj.toString(), new TypeReference<Map<String,Object>>() {});
		if(cfName == null){
			cfName = ColumnFamily.CONFIG_SETTING.getColumnFamily();
		}
		getCassandraService().addCounterRowKeyValues(cfName,keyName,dataMap);
		dataMap = new HashMap<String, Object>();
		dataMap.put("status", "Successfully added!!");
		resultList.add(dataMap);
		responseParamDTO.setContent(resultList);
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> viewSettings(String cfName, String keyName) {
		
		
		if (cfName == null) {
			cfName = ColumnFamily.CONFIG_SETTING.getColumnFamily();
		}
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
	}
	
	public ResponseParamDTO<Map<String, String>> migrateCFData(String sourceCF, String targetCF, String sourceKey, String targetKey) {

		ResponseParamDTO<Map<String, String>> responseParamDTO = new ResponseParamDTO<Map<String, String>>();
		if (!getBaseService().notNull(targetKey)) {
			targetKey = sourceKey;
		}
		List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
		if (getBaseService().notNull(sourceCF) && getBaseService().notNull(targetCF) && getBaseService().notNull(sourceKey)) {
			Map<String, String> resultMap = new HashMap<String, String>();
			OperationResult<ColumnList<String>> sourceData = getCassandraService().read(sourceCF, sourceKey);
			if (sourceData.getResult().size() > 0) {
				for (int i = 0; i < sourceData.getResult().size(); i++) {
					String columnName = sourceData.getResult().getColumnByIndex(i).getName();
					String columnValues = sourceData.getResult().getColumnByIndex(i).getStringValue();
					getCassandraService().saveProfileSettings(targetCF, targetKey, columnName, columnValues);
				}
			} else {
				throw new BadRequestException("Makesure sourceCF and sourcekey already exist");
			}
			resultMap.put("status", "Added successfully");
			resultList.add(resultMap);
		} else {
			throw new BadRequestException("Mandatory Field Missing");
		}
		responseParamDTO.setContent(resultList);
		return responseParamDTO;
	}

	private CassandraService getCassandraService() {
		return cassandraService;
	}

	private BaseService getBaseService() {
		return baseService;
	}
}
