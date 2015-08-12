package org.gooru.insights.api.services;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.spring.exception.InsightsServerException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class ItemServiceImpl implements ItemService,InsightsConstant{

	@Autowired
	private CassandraService cassandraService;
	
	@Autowired
	private BaseService baseService;
	
	@Autowired
	private SelectParamsService selectParamsService;
	
	public ResponseParamDTO<Map<String, Object>> getItemDetail(String fields, String itemIds, String startDate, String endDate, String format, String dateLevel, String granularity) throws Exception {

		try {
			fields += ",key";
			ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
			Map<String, String> selectFields = selectParamsService.getLiveDashboardData(fields);
			Collection<String> keys = new ArrayList<String>();
			Collection<String> itemKeys = new ArrayList<String>();
			if (baseService.notNull(granularity)) {
				granularity = getDate(granularity);
				for (String itemId : itemIds.split(",")) {
					keys.add(granularity + "~" + itemId);
					itemKeys.add("" + itemId);
				}
			} else {
				SimpleDateFormat givenFormat = new SimpleDateFormat(format);
				Set<String> dateKeys = performActionInCalender(givenFormat.parse(startDate), givenFormat.parse(endDate), dateLevel, getDestinationFormat(dateLevel));

				for (String dateKey : dateKeys) {
					for (String itemId : itemIds.split(",")) {
						keys.add(dateKey + "~" + itemId);
						itemKeys.add("" + itemId);
					}
				}
			}
			OperationResult<Rows<String, String>> liveDashboardResult = cassandraService.readAll(ColumnFamily.LIVE_DASHBOARD.getColumnFamily(), keys,
					new ArrayList<String>());
			List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
			for (Row<String, String> row : liveDashboardResult.getResult()) {
				Map<String, Object> dataMap = new HashMap<String, Object>();
				String[] keySplit = row.getKey().split("~");
				String rowKey = row.getKey();
				String id = row.getKey();
				rowKey = keySplit[0];
				id = keySplit[1];
				dataMap.put("key", rowKey);
				dataMap.put("id", id);
				for (Column<String> column : row.getColumns()) {
					String[] columnName = column.getName().split("~");
					if (columnName.length > 0) {
						dataMap.put(columnName[1], column.getLongValue());
					} else {
						dataMap.put(columnName[0], column.getLongValue());
					}
					dataMap.put(columnName[1], column.getLongValue());
				}
				dataList.add(dataMap);
			}
			OperationResult<Rows<String, String>> rawDataResult = cassandraService.readAll(ColumnFamily.RESOURCE.getColumnFamily(), itemKeys, new ArrayList<String>());
			List<Map<String, Object>> rawList = new ArrayList<Map<String, Object>>();
			for (Row<String, String> row : rawDataResult.getResult()) {
				Map<String, Object> dataMap = new HashMap<String, Object>();
				String[] keySplit = row.getKey().split("~");
				String id = row.getKey();
				if (keySplit.length > 1) {
					id = keySplit[1];
				} else {
					id = keySplit[0];
				}
				dataMap.put("id", id);
				for (Column<String> column : row.getColumns()) {
					String[] columnName = column.getName().split("~");
					if (columnName.length > 1) {
						dataMap.put(columnName[1], column.getStringValue());
					} else {
						dataMap.put(columnName[0], column.getStringValue());

					}
				}
				rawList.add(dataMap);
			}
			dataList = baseService.innerJoin(rawList, dataList, "id");
			dataList = baseService.properNameEndsWith(dataList, selectFields);
			dataList = formatKeyValueJson(dataList, "key");
			responseParamDTO.setContent(dataList);
			return responseParamDTO;
		} catch (Exception e) {
			throw new InsightsServerException(e.getMessage());
		}
	}
	
	private String getDate(String date){
		
		if(date.equalsIgnoreCase("all")){
			return date;
		}else if(date.equalsIgnoreCase("month")){
			return date;
		}else if(date.equalsIgnoreCase("week")){
			return date;
		}else if(date.equalsIgnoreCase("day")){
			return date;
		}else if(date.equalsIgnoreCase("year")){
			return date;
		}
		return "all";
	}
	
	private SimpleDateFormat getDestinationFormat(String dateLevel){
		
		if("MINUTE".equalsIgnoreCase(dateLevel)){
			return new SimpleDateFormat("yyyyMMddHHkk");
		}else if("HOUR".equalsIgnoreCase(dateLevel)){
			return new SimpleDateFormat("yyyyMMddHH");
		}else if("DAY".equalsIgnoreCase(dateLevel)){
			return new SimpleDateFormat("yyyyMMdd");
		}else if("MONTH".equalsIgnoreCase(dateLevel)){
			return new SimpleDateFormat("yyyyMM");
		}else if("YEAR".equalsIgnoreCase(dateLevel)){
			return new SimpleDateFormat("yyyy");
		}
		return new SimpleDateFormat("yyyyMMddHHkk");
	}
	
	private Set<String> performActionInCalender(Date startDate,Date endDate,String dateLevel,SimpleDateFormat destinationFormat){
		
		Set<String> dateKey = new TreeSet<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTime(startDate);
		while(cal.getTime().before(endDate)){
			dateKey.add(destinationFormat.format(cal.getTime()));
			if("MONTH".equalsIgnoreCase(dateLevel)){
			cal.add(Calendar.MONTH, 1);
			}else if("DAY".equalsIgnoreCase(dateLevel)){
				cal.add(Calendar.DATE, 1);
				}else if("WEEK".equalsIgnoreCase(dateLevel)){
					cal.add(Calendar.WEEK_OF_MONTH, 1);
				}else if("YEAR".equalsIgnoreCase(dateLevel)){
					cal.add(Calendar.YEAR, 1);
				}else if("HOUR".equalsIgnoreCase(dateLevel)){
					cal.add(Calendar.HOUR_OF_DAY, 1);
				}else if(dateLevel.equalsIgnoreCase("MINUTE")){
					cal.add(Calendar.MINUTE, 1);
				}
		}
		dateKey.add(destinationFormat.format(endDate));
	return dateKey;	
	}
	
	private List<Map<String, Object>> formatKeyValueJson(List<Map<String, Object>> dataMap, String key) throws Exception {

		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		JSONObject json = new JSONObject();
		Map<String, String> resultMap = new HashMap<String, String>();
		Gson gson = new Gson();
		for (Map<String, Object> map : dataMap) {
			if (map.containsKey(key)) {
				String jsonKey = map.get(key).toString();
				map.remove(key);
				json.accumulate(jsonKey, map);
			}
		}
		resultMap = gson.fromJson(json.toString(), resultMap.getClass());
		Map<String, Object> Treedata = new TreeMap<String, Object>(resultMap);
		for (Map.Entry<String, Object> entry : Treedata.entrySet()) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put(entry.getKey(), entry.getValue());
			resultList.add(map);
		}
		return resultList;
	}

	public ResponseParamDTO<Map<String, Object>> getMetadataDetails() throws Exception {

		try{
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		long totalCount = getCassandraService().read(ColumnFamily.CUSTOM_FIELDS.getColumnFamily(), "dataCount").getResult().getColumnByIndex(0).getLongValue();
		ColumnList<String> metadataCount = getCassandraService().read(ColumnFamily.CUSTOM_FIELDS.getColumnFamily(), "metadata").getResult();
		List<Map<String, Object>> metadataList = new ArrayList<Map<String, Object>>();
		long calculatedPercentage = 0L;
		for (int i = 0; i < metadataCount.size(); i++) {
			Map<String, Object> metaObject = new HashMap<String, Object>();
			String columnName = metadataCount.getColumnByIndex(i).getName();
			long count = metadataCount.getColumnByIndex(i).getLongValue();
			calculatedPercentage = (count * 100) / totalCount;
			metaObject.put("title", columnName);
			metaObject.put("metadataCount", count);
			metaObject.put("percentage", calculatedPercentage);
			metaObject.put("totalCount", totalCount);
			metadataList.add(metaObject);
		}
		responseParamDTO.setContent(metadataList);
		return responseParamDTO;
		}catch(Exception e){
			throw new InsightsServerException(e.getMessage(),e.getCause());
		}
	}
	
	private CassandraService getCassandraService() {
		return cassandraService;
	}
	
}
