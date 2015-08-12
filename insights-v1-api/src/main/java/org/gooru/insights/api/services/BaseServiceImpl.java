package org.gooru.insights.api.services;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.models.RequestParamsDTO;
import org.gooru.insights.api.spring.exception.BadRequestException;
import org.gooru.insights.api.spring.exception.InsightsServerException;
import org.gooru.insights.api.utils.InsightsLogger;
import org.gooru.insights.api.utils.JsonDeserializer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

@Service
public class BaseServiceImpl implements BaseService {

	@Autowired
	private SelectParamsService selectParamsService;

	@Autowired
	private CassandraService cassandraService;
	
	private SimpleDateFormat dateFormatter;
	
	public List<Map<String, Object>> sortBy(List<Map<String, Object>> requestData, String sortBy, String sortOrder) {

		if (notNull(sortBy)) {
			for (final String name : sortBy.split(ApiConstants.COMMA)) {
				boolean descending = false;
				if (notNull(sortOrder) && sortOrder.equalsIgnoreCase(ApiConstants.DESC)) {
					descending = true;
				}
				if (descending) {
					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
							if (m2.containsKey(name)) {
								if (m1.containsKey(name)) {
									return compareTo(m2, m1, name);
								} else {
									return 1;
								}
							} else {
								return -1;
							}
						}
					});

				} else {
					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
							if (m1.containsKey(name) && m2.containsKey(name)) {
								return compareTo(m1, m2, name);
							}
							return 1;
						}
					});
				}
			}
		}
		return requestData;
	}

	private int compareTo(Map<String, Object> m1, Map<String, Object> m2, String name) {
		if (m1.get(name) instanceof String) {
			return ((String) m1.get(name).toString().toLowerCase()).compareTo((String) m2.get(name).toString().toLowerCase());
		} else if (m1.get(name) instanceof Long) {
			return ((Long) m1.get(name)).compareTo((Long) m2.get(name));
		} else if (m1.get(name) instanceof Integer) {
			return ((Integer) m1.get(name)).compareTo((Integer) m2.get(name));
		} else if (m1.get(name) instanceof Double) {
			return ((Double) m1.get(name)).compareTo((Double) m2.get(name));
		}
		return 0;
	}	
	
	/**
	 * Check whether the user requested data is in JSON format.
	 * 
	 * @throws Exception
	 *             Invalid JSON Format
	 */
	public JSONObject validateJSON(String data) throws Exception {
		try {
			return new JSONObject(data);
		} catch (Exception e) {
			throw new BadRequestException(ErrorMessages.E100);
		}
	}
	

	public static String getValue(String key, JSONObject json) throws Exception {
		try {
			if (json.isNull(key)) {
				return null;
			}
			return json.getString(key);

		} catch (JSONException e) {
			throw new InsightsServerException(e.getMessage());
		}
	}

	public RequestParamsDTO buildRequestParameters(String data) throws Exception {

		validateJSON(data);
		try {
			RequestParamsDTO requestParamsDTO = new JsonDeserializer().deserialize(data, RequestParamsDTO.class);
			if (requestParamsDTO == null) {
				throw new BadRequestException(ErrorMessages.E100);
			}
			return requestParamsDTO;
		} catch (Exception e) {
			throw new BadRequestException(ErrorMessages.E100);
		}
	}

	public void existsFilter(RequestParamsDTO requestParamsDTO) throws Exception {
		if (requestParamsDTO.getFilters() == null) {
			throw new BadRequestException(ErrorMessages.E103 + ApiConstants.FILTERS);
		}
	}
	
	public boolean notNull(String parameter) {

		if (StringUtils.trimToNull(parameter) != null) {
			return true;
		} 
		return false;
	}
	
	public boolean notNull(Map<?, ?> request) {

		if (request != null && (!request.isEmpty())) {
			return true;
		}
		return false;
	}

	public boolean notNull(Integer parameter) {

		if (parameter != null && parameter.SIZE > 0 && (!parameter.toString().isEmpty())) {
			return true;
		}
		return false;
	}

	private SelectParamsService getSelectParameter() {

		return selectParamsService;
	}

	public long getTimeStamp(String Date) throws ParseException {
		if (Date != null && Date != "") {
			SimpleDateFormat dfm = new SimpleDateFormat("yyyy-MM-dd");
			long unixTime = 0;
			dfm.setTimeZone(TimeZone.getTimeZone("UTC"));
			unixTime = dfm.parse(Date).getTime();
			unixTime = unixTime / 1000;
			return unixTime;
		} else {
			long unixTime = 0000000000;
			return unixTime;
		}
	}

	/**
	 * To set the data type for the requested fields.
	 * get input as OperationResult ColumnList
	 * @return List of Map fields with corresponding data types values 
	 */
	public List<Map<String, Object>> getColumnValues(OperationResult<ColumnList<String>> columnList) {
		List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
		if (columnList != null && notNull(columnList.toString())) {
			/** Collection get */
			Map<String, Object> dataSet = new HashMap<String, Object>();
			for (Column<String> column : columnList.getResult()) {
				try{
				if (column.getName().endsWith("~grade_in_percentage") || column.getName().endsWith("~question_count") || column.getName().endsWith("~views")
						|| column.getName().endsWith("~avg_time_spent") || column.getName().endsWith("~time_spent") || column.getName().endsWith("~A") || column.getName().endsWith("~B")
						|| column.getName().endsWith("~C") || column.getName().endsWith("~D") || column.getName().endsWith("~E") || column.getName().endsWith("~RA")
						|| column.getName().endsWith("question_id") || column.getName().endsWith("~skipped") || column.getName().endsWith("deleted") || column.getName().endsWith("~avg_reaction")
						|| column.getName().endsWith("~skipped~status") || column.getName().endsWith("~score") || column.getName().endsWith("~tau") || column.getName().endsWith("~in-correct")
						|| column.getName().endsWith("~correct") || column.getName().endsWith("item_sequence") || column.getName().endsWith("is_required") || column.getName().endsWith("status") || column.getName().endsWith("item_sequence")
						|| column.getName().endsWith("attempts") || column.getName().endsWith("~feed_back_timestamp") || column.getName().endsWith("~feed_back_time_spent") || column.getName().endsWith("content_id") || column.getName().endsWith("contentId")) {
					dataSet.put(column.getName(), (notNull(String.valueOf(column.getLongValue()))) ? column.getLongValue() : 0);
				} else if (column.getName().endsWith("lastModified") || column.getName().endsWith("createdOn") || column.getName().endsWith("association_date") || column.getName().endsWith("lastAccessed") ) { 
					dataSet.put(column.getName(), column.getDateValue().getTime());
				} else if(column.getName().endsWith("statistics.hasFrameBreakerN")){
					dataSet.put(column.getName(), column.getIntegerValue());
				}else if (column.getName().endsWith("isDeleted")){ 
					dataSet.put(column.getName(), column.getBooleanValue() ? 1 : 0);
				}else {
					dataSet.put(column.getName(), column.getStringValue());
				}
				}catch(Exception e){
					InsightsLogger.error(e);
				}
			}
			resultSet.add(dataSet);
		}
		return resultSet;
	}

	/**
	 * To set the data type for the requested fields.
	 * get input as OperationResult ColumnList
	 * @return Map fields with corresponding data types values 
	 */
	public Map<String, Object> getColumnValue(OperationResult<ColumnList<String>> columnList) {
		Map<String, Object> dataSet = new HashMap<String, Object>();
		if (columnList != null && notNull(columnList.toString())) {
			for (Column<String> column : columnList.getResult()) {
				try{
				if (column.getName().endsWith("~grade_in_percentage") || column.getName().endsWith("~question_count") || column.getName().endsWith("~views")
						|| column.getName().endsWith("~avg_time_spent") || column.getName().endsWith("~time_spent") || column.getName().endsWith("~A") || column.getName().endsWith("~B")
						|| column.getName().endsWith("~C") || column.getName().endsWith("~D") || column.getName().endsWith("~E") || column.getName().endsWith("~RA")
						|| column.getName().endsWith("question_id") || column.getName().endsWith("~skipped") || column.getName().endsWith("deleted") || column.getName().endsWith("~avg_reaction")
						|| column.getName().endsWith("~skipped~status") || column.getName().endsWith("~score") || column.getName().endsWith("~tau") || column.getName().endsWith("~in-correct")
						|| column.getName().endsWith("~correct") || column.getName().endsWith("item_sequence")  || column.getName().endsWith("is_required") || column.getName().endsWith("status") || column.getName().endsWith("item_sequence")
						|| column.getName().endsWith("attempts") || column.getName().endsWith("~feed_back_timestamp")|| column.getName().endsWith("~feed_back_time_spent") || column.getName().endsWith("~question_status")
						|| column.getName().endsWith("content_id")) {
					dataSet.put(column.getName(), (notNull(String.valueOf(column.getLongValue()))) ? column.getLongValue() : 0);

				} else if(column.getName().endsWith("statistics.hasFrameBreakerN")){
					dataSet.put(column.getName(), column.getIntegerValue());
				}  else if (column.getName().endsWith("lastModified") || column.getName().endsWith("createdOn") || column.getName().endsWith("association_date") || column.getName().endsWith("lastAccessed")) { 
					dataSet.put(column.getName(), column.getDateValue().getTime());
				}else {
					dataSet.put(column.getName(), column.getStringValue());
				}
				}catch(Exception e){
					InsightsLogger.error(e);
				}
			}
		}
		return dataSet;
	}

	public Map<String, Object> getLongValue(OperationResult<ColumnList<String>> columnList) {
		Map<String, Object> dataSet = new HashMap<String, Object>();
		if (columnList != null && notNull(columnList.toString())) {
			for (Column<String> column : columnList.getResult()) {
				try{
				dataSet.put(column.getName(), (notNull(String.valueOf(column.getLongValue()))) ? column.getLongValue() : 0);
			}catch(Exception e){
			InsightsLogger.error(e);	
			}
			}
		}
		return dataSet;
	}
	
	public Map<String, String> getStringValue(OperationResult<ColumnList<String>> columnList) {
		Map<String, String> dataSet = new HashMap<String, String>();
		if (columnList != null && notNull(columnList.toString())) {
			for (Column<String> column : columnList.getResult()) {
				dataSet.put(column.getName(), (notNull(column.getStringValue())) ? column.getStringValue() : null);
			}
		}
		return dataSet;
	}
	
	public Map<String, Object> getRowLongValue(OperationResult<Rows<String, String>> rowList) {
		Map<String, Object> resultSetMap = new LinkedHashMap<String, Object>();
		if (rowList != null && !rowList.getResult().isEmpty()) {
			Rows<String, String> row = rowList.getResult();
			if (row != null && !row.isEmpty()) {
				for (int i = 0; i < row.size(); i++) {
					for (Column<String> column : row.getRowByIndex(i).getColumns()) {
						try{
						resultSetMap.put(row.getRowByIndex(i).getKey(),column.getLongValue());
						}catch(Exception e){
							InsightsLogger.error(e);
						}
					}
				}
			}
		}
		return resultSetMap;
	}
	
	public Map<String, Object> getRowLongValues(OperationResult<Rows<String, String>> rowList) {
		Map<String, Object> resultSetMap = new LinkedHashMap<String, Object>();
		if (rowList != null && !rowList.getResult().isEmpty()) {
			Rows<String, String> row = rowList.getResult();
			if (row != null && !row.isEmpty()) {
				for (int i = 0; i < row.size(); i++) {
					Map<String, Object> columnSet = new LinkedHashMap<String, Object>();
					for (Column<String> column : row.getRowByIndex(i).getColumns()) {
						try{
						columnSet.put(column.getName(), column.getLongValue());
						}catch(Exception e){
							InsightsLogger.error(e);
						}
					}
					resultSetMap.put(row.getRowByIndex(i).getKey(),columnSet);
				}
			}
		}
		return resultSetMap;
	}
	public Map<String, Object> getColumnValues(OperationResult<ColumnList<String>> columnList, Map<String, Object> key) {
		Map<String, Object> dataSet = new HashMap<String, Object>();
		if (notNull(columnList.toString())) {
			for (Column<String> column : columnList.getResult()) {
				try{
				if (column.getName().endsWith("~views") || column.getName().endsWith("~avg_time_spent") || column.getName().endsWith("~time_spent") || column.getName().endsWith("~A")
						|| column.getName().endsWith("~B") || column.getName().endsWith("~C") || column.getName().endsWith("~D") || column.getName().endsWith("~E") || column.getName().endsWith("~RA")
						|| column.getName().endsWith("question_id") || column.getName().endsWith("~skipped") || column.getName().endsWith("deleted") || column.getName().endsWith("~avg_reaction")
						|| column.getName().endsWith("~skipped~status") || column.getName().endsWith("~score") || column.getName().endsWith("~tau") || column.getName().endsWith("~in-correct")
						|| column.getName().endsWith("~correct") || column.getName().endsWith("item_sequence") 
						|| column.getName().endsWith("~feed_back_time_spent") || column.getName().endsWith("~question_status") || column.getName().endsWith("status")
						|| column.getName().endsWith("item_sequence")  || column.getName().endsWith("is_required") || column.getName().endsWith("attempts") || column.getName().endsWith("~feed_back_timestamp") || column.getName().endsWith("content_id")) {
					dataSet.put(column.getName(), (notNull(String.valueOf(column.getLongValue()))) ? column.getLongValue() : 0);
			 } else if(column.getName().endsWith("statistics.hasFrameBreakerN")){
					dataSet.put(column.getName(), column.getIntegerValue());
				}else if(column.getName().endsWith("lastModified") || column.getName().endsWith("createdOn") || column.getName().endsWith("association_date") || column.getName().endsWith("lastAccessed")){
				 dataSet.put(column.getName(), column.getDateValue().getTime());
				}else {
					dataSet.put(column.getName(), column.getStringValue());
				}
				}catch(Exception e){
					InsightsLogger.error(e);
				}
			}

			dataSet.putAll(key);

			return dataSet;
		}
		return dataSet;
	}
	
	/**
	 * To set the data type for the requested fields.
	 * get input as OperationResult Rows
	 * @return List of Map fields with corresponding data types values 
	 */
		public List<Map<String, Object>> getRowsColumnValues(OperationResult<Rows<String, String>> rowList) {
			List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
		if (rowList != null && !rowList.getResult().isEmpty()) {
			Rows<String, String> row = rowList.getResult();
			if (!row.isEmpty()) {
				for (int i = 0; i < row.size(); i++) {
					String key = row.getRowByIndex(i).getKey();
					Map<String, Object> dataSet = new HashMap<String, Object>();
					/** Grade info */
					for (Column<String> column : row.getRowByIndex(i).getColumns()) {
						try{
						if ((column.getName().endsWith("~grade_in_percentage") || column.getName().endsWith("~question_count") || column.getName().endsWith("~views")
								|| column.getName().endsWith("~avg_time_spent") || column.getName().endsWith("~time_spent") || column.getName().endsWith("~A") || column.getName().endsWith("~B")
								|| column.getName().endsWith("is_group_owner") || column.getName().endsWith("question_id") || column.getName().endsWith("~C") || column.getName().endsWith("~D")
								|| column.getName().endsWith("~E") || column.getName().endsWith("answer_id") || column.getName().endsWith("sequence") || column.getName().endsWith("is_correct")
								|| column.getName().endsWith("~skipped") || column.getName().endsWith("deleted") || column.getName().endsWith("active_flag")
								|| column.getName().endsWith("~avg_reaction") || column.getName().endsWith("~RA") || column.getName().endsWith("~feed_back_timestamp") || column.getName().endsWith("~skipped~status")
								|| column.getName().endsWith("~feed_back_time_spent")  || column.getName().endsWith("is_required") || column.getName().endsWith("~question_status") || column.getName().endsWith("~score")
								|| column.getName().endsWith("~tau") || column.getName().endsWith("~in-correct") || column.getName().endsWith("~correct") || column.getName().endsWith("item_sequence")
								|| column.getName().endsWith("status") || column.getName().endsWith("item_sequence") || column.getName().endsWith("~attempts"))) {
							if (!column.getName().endsWith("collection_status")) {
								dataSet.put(column.getName(),
										column.getLongValue());
							} else {

								dataSet.put(column.getName(),
										column.getStringValue());
							}
						} else if(column.getName().endsWith("statistics.hasFrameBreakerN")){
							dataSet.put(column.getName(), column.getIntegerValue());
						} else if(column.getName().endsWith("lastModified") || column.getName().endsWith("createdOn") || column.getName().endsWith("association_date") || column.getName().endsWith("lastAccessed") || column.getName().endsWith("endTime")){
							dataSet.put(column.getName(), column.getDateValue().getTime());
						} else if(column.getName().endsWith("isDeleted") || column.getName().endsWith("isDeleted")){
							try {
								dataSet.put(column.getName(), column.getBooleanValue() ? 1 : 0);
							} catch ( Exception e) {
								dataSet.put(column.getName(), column.getLongValue());
							}
						
						} else if(column.getName().equalsIgnoreCase("question.type") || column.getName().equalsIgnoreCase("question.questionType")){
							dataSet.put("type", column.getStringValue());
						}
						else {
							dataSet.put("key", key);
                             dataSet.put(column.getName(), column.getStringValue());
						}
					}catch(Exception e){
						InsightsLogger.error(e);
					}
					}
					if(!dataSet.isEmpty()){
					resultSet.add(dataSet);
					}
				}
			}
		}
		return resultSet;
	}

	public List<Map<String, Object>> innerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String commonKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (!child.isEmpty() && !parent.isEmpty()) {
			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> parentEntry : parent) {
					if (childEntry.containsKey(commonKey) && parentEntry.containsKey(commonKey)) {
						if (childEntry.get(commonKey).equals(parentEntry.get(commonKey))) {
							childEntry.remove(commonKey);
							appended.putAll(childEntry);
							appended.putAll(parentEntry);
							break;
						}
					}
				}
				resultList.add(appended);
			}
			return resultList;
		}
		return resultList;
	}

	public List<Map<String, Object>> innerJoinWithContainsKey(
			List<Map<String, Object>> parent, List<Map<String, Object>> child,
			String parentKey, String childKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (!child.isEmpty() && !parent.isEmpty()) {
			for (Map<String, Object> childEntry : child) {
				for (Map<String, Object> parentEntry : parent) {
					Map<String, Object> appended = new HashMap<String, Object>();

					for (Map.Entry<String, Object> entry : parentEntry
							.entrySet()) {
						if (entry.getKey().contains(parentKey)) {
							if (childEntry.get(childKey).toString()
									.contains(entry.getValue().toString())) {
								if (childEntry.containsKey("question_type")) {
									appended.put("question_type", childEntry
											.get("question_type").toString());
								}
								appended.putAll(parentEntry);
							}
						}
					}
					if(!appended.isEmpty()){
					resultList.add(appended);
					}
				}

			}
		}
		return resultList;
	}

	public List<Map<String, Object>> innerJoin(List<Map<String, Object>> leftSideRecords, List<Map<String, Object>> rightSideRecords,
												String leftRecordKeyName, String rightRecordKeyName) {
		List<Map<String, Object>> mergedRecords = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> rightSideRecord : rightSideRecords) {
			Map<String, Object> mergedRecord = new HashMap<String, Object>();
			for (Map<String, Object> leftSideRecord : leftSideRecords) {
				if (rightSideRecord.get(rightRecordKeyName) != null && leftSideRecord.get(leftRecordKeyName) != null && rightSideRecord.get(rightRecordKeyName).equals(leftSideRecord.get(leftRecordKeyName))) {
						mergedRecord.putAll(rightSideRecord);
						mergedRecord.putAll(leftSideRecord);
						break;
				}
			}
			mergedRecords.add(mergedRecord);
		}
		return mergedRecords;
	}

	public List<Map<String, Object>> rightJoin(List<Map<String, Object>> leftSideRecords, List<Map<String, Object>> rightSideRecords, String leftRecordKeyName, String rightRecordKeyName) {
		List<Map<String, Object>> mergedRecords = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> rightSideRecord : rightSideRecords) {
			boolean merged = false;
			Map<String, Object> mergedRecord = new HashMap<String, Object>();
			for (Map<String, Object> leftSideRecord : leftSideRecords) {
				if (rightSideRecord.get(rightRecordKeyName) != null && leftSideRecord.get(leftRecordKeyName) != null && rightSideRecord.get(rightRecordKeyName).equals(leftSideRecord.get(leftRecordKeyName))) {
						merged = true;
						mergedRecord.putAll(rightSideRecord);
						mergedRecord.putAll(leftSideRecord);
						break;
				}
			}
			if (!merged) {
				mergedRecord.putAll(rightSideRecord);
			}
			mergedRecords.add(mergedRecord);
		}
		return mergedRecords;
	}

	public List<Map<String, Object>> leftJoin(List<Map<String, Object>> leftSideRecords, List<Map<String, Object>> rightSideRecords, String leftRecordKeyName, String rightRecordKeyName) {
		List<Map<String, Object>> mergedRecords = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> leftSideRecord : leftSideRecords) {
			boolean merged = false;
			Map<String, Object> mergedRecord = new HashMap<String, Object>();
			if(rightSideRecords != null && !rightSideRecords.isEmpty()) {
				for (Map<String, Object> rightSideRecord : rightSideRecords) {
					if (rightSideRecord.containsKey(rightRecordKeyName) && leftSideRecord.containsKey(leftRecordKeyName)) {
						if (rightSideRecord.get(rightRecordKeyName).equals(leftSideRecord.get(leftRecordKeyName))) {
							merged = true;
							mergedRecord.putAll(rightSideRecord);
							mergedRecord.putAll(leftSideRecord);
							break;
						}
					}
				}
			}
			if (!merged) {
				mergedRecord.putAll(leftSideRecord);
			}
			mergedRecords.add(mergedRecord);
		}
		return mergedRecords;
	}

	public List<Map<String, Object>> leftJoinwithTwoKey(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> parentEntry : parent) {
			boolean occured = false;
			Map<String, Object> appended = new HashMap<String, Object>();
			for (Map<String, Object> childEntry : child) {
				if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey) && childEntry.containsKey(parentKey) && parentEntry.containsKey(childKey)) {
					if (childEntry.get(parentKey).equals(parentEntry.get(parentKey)) && parentEntry.get(childKey).equals(childEntry.get(childKey))) {
						occured = true;
						appended.putAll(childEntry);
						appended.putAll(parentEntry);
						break;
					}
				}
			}
			if (!occured) {
				appended.putAll(parentEntry);
			}

			resultList.add(appended);
		}
		return resultList;
	}

	public List<Map<String, Object>> removeUnknownKeyList(List<Map<String, Object>> requestData, String key, String exceptionalkey, Object value, boolean status) {
		List<Map<String, Object>> responseData = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> map : requestData) {
			if (map.containsKey(key)) {
				responseData.add(map);
			} else if (map.containsKey(exceptionalkey)) {
				if (map.get(exceptionalkey).toString().equalsIgnoreCase(value.toString()) == status) {
					responseData.add(map);
				}
			}

		}
		return responseData;
	}

	/**
	 * 
	 */
	public List<Map<String, Object>> combineTwoList(List<Map<String, Object>> parentList, List<Map<String, Object>> childList, String parentKey, String childKey) {

		List<Map<String, Object>> responseList = new ArrayList<Map<String, Object>>();
		// //no of resource
		for (Map<String, Object> parentMap : parentList) {
			// //no of users
			for (Map<String, Object> childMap : childList) {
				Map<String, Object> responseMap = new HashMap<String, Object>();
				responseMap.putAll(childMap);
				responseMap.put(parentKey, parentMap.get(parentKey));
				responseList.add(responseMap);
			}
		}
		return responseList;
	}

	public List<Map<String, Object>> removeUnknownValueList(List<Map<String, Object>> requestData, String key, String validateValue, String exceptionalkey, Object value, boolean status) {
		List<Map<String, Object>> responseData = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> map : requestData) {
			if (map.containsKey(key)) {
				if (map.get(key).toString().equalsIgnoreCase(validateValue)) {
					responseData.add(map);
				} else if (map.containsKey(exceptionalkey)) {
					if (map.get(exceptionalkey).toString().equalsIgnoreCase(value.toString()) == status) {
						responseData.add(map);
					}
				}
			} else if (map.containsKey(exceptionalkey)) {
				if (map.get(exceptionalkey).toString().equalsIgnoreCase(value.toString()) == status) {
					responseData.add(map);
				}
			}
		}
		return responseData;
	}

	public List<Map<String, Object>> safeJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();

		if (!parent.isEmpty()) {
			for (Map<String, Object> parentEntry : parent) {
				boolean occured = false;
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> childEntry : child) {
					if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey)) {
						if (childEntry.get(childKey).equals(parentEntry.get(parentKey))) {
							occured = true;
							appended.putAll(childEntry);
							appended.putAll(parentEntry);
							break;
						}
					}
				}
				if (!occured) {
					appended.putAll(parentEntry);
				}

				resultList.add(appended);
			}
			List<Map<String, Object>> finalList = new ArrayList<Map<String, Object>>(resultList);

			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> parentEntry : finalList) {
					if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey)) {
						if (childEntry.get(childKey).equals(parentEntry.get(parentKey))) {
							break;
						} else {
							appended.putAll(childEntry);
						}
					} else {
						appended.putAll(childEntry);
					}
				}
				resultList.add(appended);
			}
			return resultList;
		} else if (!child.isEmpty()) {
			for (Map<String, Object> parentEntry : child) {
				boolean occured = false;
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> childEntry : parent) {
					if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey)) {
						if (childEntry.get(childKey).equals(parentEntry.get(parentKey))) {
							occured = true;
							appended.putAll(childEntry);
							appended.putAll(parentEntry);
							break;
						}
					}
				}
				if (!occured) {
					appended.putAll(parentEntry);
				}

				resultList.add(appended);
			}
			List<Map<String, Object>> finalList = new ArrayList<Map<String, Object>>(resultList);

			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> parentEntry : finalList) {
					if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey)) {
						if (childEntry.get(childKey).equals(parentEntry.get(parentKey))) {
							break;
						} else {
							appended.putAll(childEntry);
						}
					} else {
						appended.putAll(childEntry);
					}
				}
				resultList.add(appended);
			}
			return resultList;
		}
		return resultList;
	}

	/**
	 * Append the resource gooru oid with the requested fields to get the data
	 */

	   public Collection<String> generateCommaSeparatedStringToKeys(String fields, String prefix, String suffix) {
	       Collection<String> resultFields = new ArrayList<String>();
	       if (notNull(fields)) {
	           for (String field : fields.split(ApiConstants.COMMA)) {
	               if (notNull(prefix) && notNull(suffix)) {
	                   for (String first : prefix.split(ApiConstants.COMMA)) {
	                       StringBuffer resultField = new StringBuffer();
	                       resultField.append(first);
	                       resultField.append(field);
	                       for (String last : suffix.split(ApiConstants.COMMA)) {
	                           resultFields.add(resultField.toString() + last);
	                       }
	                   }
	               } else if (notNull(prefix) && StringUtils.isBlank(suffix)) {
	                   for (String first : prefix.split(ApiConstants.COMMA)) {
	                       StringBuffer resultField = new StringBuffer();
	                       resultField.append(first);
	                       resultField.append(field);
	                       resultFields.add(resultField.toString());
	                   }
	               } else if (StringUtils.isBlank(prefix) && notNull(ApiConstants.COMMA)) {
	                   for (String first : suffix.split(ApiConstants.COMMA)) {
	                       StringBuffer resultField = new StringBuffer();
	                       resultField.append(field);
	                       resultField.append(first);
	                       resultFields.add(resultField.toString());
	                   }
	               } else {
	                   resultFields.add(field);
	               }
	           }
	       }
	       return resultFields;
	   }

	public Collection<String> generateCommaSeparatedStringToKeys(String selectFields, String prefix, Collection<String> suffix) {
		Collection<String> resultFields = new ArrayList<String>();
		if (notNull(selectFields)) {
			for (String field : selectFields.split(ApiConstants.COMMA)) {
				if (notNull(prefix) && !suffix.isEmpty()) {
					for (String first : prefix.split(ApiConstants.COMMA)) {
						StringBuffer resultField = new StringBuffer();
						resultField.append(first);
						resultField.append(field);
						for (String last : suffix) {
							resultFields.add(resultField.toString() + last);
						}
					}
				} else if (notNull(prefix) && suffix.isEmpty()) {
					for (String first : prefix.split(ApiConstants.COMMA)) {
						StringBuffer resultField = new StringBuffer();
						resultField.append(first);
						resultField.append(field);
						resultFields.add(resultField.toString());
					}
				} else if (!notNull(prefix) && !suffix.isEmpty()) {
					for (String first : suffix) {
						StringBuffer resultField = new StringBuffer();
						resultField.append(field);
						resultField.append(first);
						resultFields.add(resultField.toString());
					}
				} else {
					resultFields.add(field);
				}
			}
		}
		return resultFields;
	}

	public Collection<String> convertStringToCollection(String field) {
		Collection<String> includedData = new ArrayList<String>();
		for (String value : field.split(ApiConstants.COMMA)) {
			includedData.add(value);
		}
		return includedData;
	}

	public List<Map<String, Object>> injectMapRecord(List<Map<String, Object>> records, Map<String, Object> injuctableRecord) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> record : records) {
			record.putAll(injuctableRecord);
			resultList.add(record);
		}
		return resultList;
	}

	public List<Map<String,Object>> formatRecord(List<Map<String,Object>> rawData,Map<String,Object> injuctableRecord,String formatKey,String id){
		
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		for(Map<String,Object> map : rawData){
			Map<String,Object> dataMap = new HashMap<String, Object>();
			for(Map.Entry<String, Object> entry : injuctableRecord.entrySet()){
				if(map.containsKey(formatKey) ){
				dataMap.put(entry.getKey(),entry.getValue().toString().replaceAll(id,map.get(formatKey).toString()));
			}
			}
			dataMap.putAll(map);
			resultList.add(dataMap);
		}
		return resultList;
	}

	public List<Map<String, Object>> injectCounterRecord(List<Map<String, Object>> aggregateData, Map<String, Object> injuctableRecord) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		int frequencyRecord = 0;
		for (Map<String, Object> map : aggregateData) {
			Map<String, Object> resultMap = new HashMap<String, Object>();
			for (Map.Entry<String, Object> value : map.entrySet()) {
				resultMap.put(value.getKey(), value.getValue());
			}
			frequencyRecord += injuctableRecord.get("counter") != null ? Integer.parseInt(injuctableRecord.get("counter").toString()) : 1;
			resultMap.put(String.valueOf(injuctableRecord.get("key")), frequencyRecord);
			resultList.add(resultMap);
		}
		return resultList;
	}

	/**
	 * Get aggregated data and user data and add these in to the single list
	 */
	public List<Map<String, Object>> RandomJoin(List<Map<String, Object>> record1, List<Map<String, Object>> record2) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> map : record1) {
			Map<String, Object> resultMap = new HashMap<String, Object>();
			for (Map<String, Object> map2 : record2) {
				resultMap.putAll(map2);
			}
			resultMap.putAll(map);
			resultList.add(resultMap);
		}
		return resultList;
	}

	public List<Map<String, Object>> getData(List<Map<String, Object>> requestData, String coreKey) {
		boolean firstEntry = false;
		List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> finalSet = new ArrayList<Map<String, Object>>();
		Collections.sort(requestData, new Comparator<Map<String, Object>>() {
			public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
				return String.valueOf(m1.get("question_gooru_oid")).compareTo(String.valueOf(m2.get("question_gooru_oid")));
			}
		});
		String gooruOId = "";
		for (Map<String, Object> map : requestData) {
			Map<String, Object> resultMap = new HashMap<String, Object>();
			Map<String, Object> resultMaps = new TreeMap<String, Object>(map);
			for (Map.Entry<String, Object> value : resultMaps.entrySet()) {
				if (value.getKey().equalsIgnoreCase(coreKey)) {
					if (firstEntry) {
						if (gooruOId.equalsIgnoreCase(String.valueOf(value.getValue()))) {
						} else {
							Map<String, Object> intermediateMap = new HashMap<String, Object>();
							intermediateMap.put(coreKey, gooruOId);
							Collections.sort(resultSet, new Comparator<Map<String, Object>>() {
								public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
									return String.valueOf(m1.get("sequence")).compareTo(String.valueOf(m2.get("sequence")));
								}
							});
							intermediateMap.put("metaData", resultSet);

							finalSet.add(intermediateMap);
							resultSet = new ArrayList<Map<String, Object>>();
							gooruOId = String.valueOf(value.getValue());

						}
					} else {
						gooruOId = String.valueOf(value.getValue());
					}
					firstEntry = true;
				}
				if(!value.getKey().equalsIgnoreCase("key")) {
					resultMap.put(value.getKey(), value.getValue());
				}
			}
			resultSet.add(resultMap);
		}
		Map<String, Object> intermediateMap = new HashMap<String, Object>();
		intermediateMap.put(coreKey, gooruOId);
		intermediateMap.put("metaData", resultSet);
		Collections.sort(resultSet, new Comparator<Map<String, Object>>() {
			public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
				return String.valueOf(m1.get("sequence")).compareTo(String.valueOf(m2.get("sequence")));
			}
		});

		finalSet.add(intermediateMap);
		return finalSet;
	}

	public List<Map<String, Object>> getUserData(List<Map<String, Object>> requestData, String coreKey, Map<String, String> selectValue,String sortBy,String sortOrder,Integer limit) {
		boolean firstEntry = false;
		List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> finalSet = new ArrayList<Map<String, Object>>();
		String gooruOId = "";
		for (Map<String, Object> map : requestData) {
			Map<String, Object> resultMap = new HashMap<String, Object>();
			Map<String, Object> resultMaps = new TreeMap<String, Object>(map);
			for (Map.Entry<String, Object> value : resultMaps.entrySet()) {
				if (value.getKey().equalsIgnoreCase(coreKey)) {
					if (firstEntry) {
						if (gooruOId.equalsIgnoreCase(String.valueOf(value.getValue()))) {
						} else {
							Map<String, Object> intermediateMap = new LinkedHashMap<String, Object>();
							intermediateMap.put(coreKey, gooruOId);
							intermediateMap.put("userData", resultSet);

							finalSet.add(intermediateMap);
							resultSet = new ArrayList<Map<String, Object>>();
							gooruOId = String.valueOf(value.getValue());

						}
					} else {
						gooruOId = String.valueOf(value.getValue());
					}
					firstEntry = true;
				}
				resultMap.put(value.getKey(), value.getValue());
			}
			resultSet.add(resultMap);
		}
		Map<String, Object> intermediateMap = new HashMap<String, Object>();
		intermediateMap.put(coreKey, gooruOId);
		intermediateMap.put("userData",resultSet);

		finalSet.add(intermediateMap);
		finalSet = selfJoin(finalSet, coreKey, "userData",sortBy,sortOrder,limit);
		return finalSet;
	}

	public List<Map<String, Object>> selfJoin(List<Map<String, Object>> requestData, String key, String appendField,String sortBy,String sortOrder,Integer limit) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		String value = "";
		boolean firstEntry = false;
		for (Map<String, Object> map : requestData) {
			if (!firstEntry) {
				firstEntry = true;
			}
			if (!value.contains(String.valueOf(map.get(key)))) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
				List<Map<String, Object>> gottenValue = new ArrayList<Map<String, Object>>();
				for (Map<String, Object> map2 : requestData) {
					if (map.get(key).equals(map2.get(key))) {

						gottenValue.addAll((Collection<? extends Map<String, Object>>) map2.get(appendField));
						value += "'" + map2.get(key) + "'";
					}
				}

				resultMap.put(key, map.get(key));
				gottenValue = sortBy(new ArrayList<Map<String,Object>>(new HashSet(gottenValue)), sortBy, sortOrder);
				if(notNull(limit)){
					if(!gottenValue.isEmpty() && gottenValue.size() >= limit)
					gottenValue = gottenValue.subList(0, limit);
				}
				resultMap.put(appendField,gottenValue);
				resultList.add(resultMap);
			}
			
		}
		return resultList;
	}

	public Comparator<Map<String, Object>> mapComparator = new Comparator<Map<String, Object>>() {
		public int compare(Map<String, Object> m1, Map<String, Object> m2) {
			return String.valueOf(m1.get("question_gooru_oid")).compareTo(String.valueOf(m2.get("question_gooru_oid")));
		}
	};

	/**
	 * 	Accumulate all the resource ids in single unit with List of Map and String as arguments 
	 */
	public StringBuffer getCommaSeparatedIds(List<Map<String, Object>> requestData, String requestKey) {
			StringBuffer exportData = new StringBuffer();
			if(requestData != null && !requestData.isEmpty()){
				for (Map<String, Object> map : requestData) {
						if (map.containsKey(requestKey) && map.get(requestKey) != null) {
							if(exportData.length() > 0) {
								exportData.append(ApiConstants.COMMA);
							} 
							exportData.append(map.get(requestKey));
						}
				}
			}
			return exportData;
	}

	/**
	 * 	Accumulate all the resource ids in single unit with List of Map and Collection as arguments 
	 */
	public Map<String, String> getCommaSeparatedIds(List<Map<String, Object>> requestData, Collection<String> requestKey) {
		Map<String, String> exportData = new HashMap<String, String>();
		String appendedField = ApiConstants.STRING_EMPTY;
		for (Map<String, Object> map : requestData) {
			for (String field : requestKey) {
				if(map.get(field) != null)
					if (map.containsKey(field)) {
						if (exportData.get(field) != null && !exportData.get(field).isEmpty()) {
							appendedField +=exportData.get(field)+",";
							exportData.put(field,appendedField);
						}
						exportData.put(field,""+appendedField+map.get(field));
					} 
			}
		}
		return exportData;
	}

	public List<Map<String, Object>> JoinWithSingleKey(List<Map<String, Object>> multipleRecord, List<Map<String, Object>> singleRecord, String MultipleRecordKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (notNull(MultipleRecordKey)) {
			if (!multipleRecord.isEmpty()) {

				for (Map<String, Object> multpileEntry : multipleRecord) {
					Map<String, Object> resultMap = new HashMap<String, Object>();
					for (Map<String, Object> singleEntry : singleRecord) {
						for (Map.Entry<String, Object> record : singleEntry.entrySet()) {
							if (record.getKey().contains(String.valueOf(multpileEntry.get(MultipleRecordKey)))) {
								resultMap.put(record.getKey(), record.getValue());
							}
						}

					}
					resultMap.putAll(multpileEntry);
					resultList.add(resultMap);
				}
				return resultList;
			}
		}
		return resultList;
	}

	public List<Map<String, Object>> getSingleKey(List<Map<String, Object>> singleRecord, String keyName, String RecordKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (notNull(RecordKey)) {
			if (!singleRecord.isEmpty()) {

				for (Map<String, Object> singleEntry : singleRecord) {
					Set<String> gooruOId = new HashSet<String>();
					Set<String> keys = singleEntry.keySet();
					for (String key : keys) {
						if (key.endsWith(RecordKey)) {
							String value = key.replaceAll(RecordKey, "");
							gooruOId.add(value);
						}
					}
					for (String id : gooruOId) {
						Map<String, Object> resultMap = new HashMap<String, Object>();
						for (Map.Entry<String, Object> map : singleEntry.entrySet()) {
							if (map.getKey().contains(id)) {
								resultMap.put(map.getKey(), map.getValue());
								resultMap.put("gooru_uid", singleEntry.get("gooru_uid"));
							}
						}

						resultList.add(resultMap);
					}
				}
				return resultList;
			}
		}
		return resultList;
	}

	public List<Map<String, Object>> properName(List<Map<String, Object>> requestList, Map<String, String> columnNames) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (!requestList.isEmpty()) {
			for (Map<String, Object> map : requestList) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
				for (Map.Entry<String, Object> entry : map.entrySet()) {
					for (Map.Entry<String, String> column : columnNames.entrySet()) {
						if (entry.getKey().contains(column.getValue())) {
							resultMap.put(column.getKey(), entry.getValue());
							break;
						}
					}
				}
				resultList.add(resultMap);
			}
			return resultList;
		}
		return resultList;
	}
	
	public List<Map<String, Object>> properNameEndsWith(List<Map<String, Object>> requestList, Map<String, String> columnNames) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (!requestList.isEmpty()) {
			for (Map<String, Object> map : requestList) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
				for (Map.Entry<String, Object> entry : map.entrySet()) {
					for (Map.Entry<String, String> column : columnNames.entrySet()) {
						if (entry.getKey().endsWith(column.getValue())) {
							resultMap.put(column.getKey(), entry.getValue());
							break;
						}
					}
				}
				resultList.add(resultMap);
			}
			return resultList;
		}
		return resultList;
	}

	public List<Map<String, Object>> buildJSON(List<Map<String, Object>> resultSet,Collection<String> additionParameter, Map<String, String> surName,
			boolean checkSession) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> map : resultSet) {
			boolean isChoice = false;
			boolean hasQuestion = false;
			Map<String,Long> tempMap = new HashMap<String,Long>();
			Map<String, Object> resultMap = new HashMap<String, Object>();
			if (map.containsKey("question_type")) {
				hasQuestion = true;
			}
			if(hasQuestion){
			if (map.get("question_type").toString().equalsIgnoreCase("MC") || map.get("question_type").toString().equalsIgnoreCase("TF")) {
				isChoice = true;
			}
			}
			if (isChoice && checkSession) {
				for (Map.Entry<String, Object> entry : map.entrySet()) {
					for (String column : additionParameter) {
						if(entry.getKey().endsWith("~options")){
								if(!entry.getValue().toString().equalsIgnoreCase("skipped")){
									tempMap.put(entry.getValue().toString(), Long.valueOf(1));
								}
							resultMap.put(column, tempMap);
						}
					}
				}
			} else {
				for (Map.Entry<String, Object> entry : map.entrySet()) {
					for (String column : additionParameter) {
							String data[] = entry.getKey().split("~");
								if (surName.containsKey("~" + data[data.length - 1])) {
										tempMap.put(surName.get("~" + data[data.length - 1]), Long.parseLong(String.valueOf(entry.getValue())));
								}
						resultMap.put(column, tempMap);
					}
				}
			}
			resultMap.putAll(map);
			resultList.add(resultMap);
		}
		return resultList;
	}

	public String getOptionSequence(String value) {
		if (value.contains("1"))
			return "A";
		if (value.contains("2"))
			return "B";
		if (value.contains("3"))
			return "C";
		if (value.contains("4"))
			return "D";
		if (value.contains("5"))
			return "E";
		return "F";
	}

	public List<Map<String, Object>> changeDataType(Map<String, String> changableDataType, List<Map<String, Object>> requestList) {
		List<Map<String, Object>> changedList = new ArrayList<Map<String, Object>>();

		for (Map<String, Object> map : requestList) {
			Map<String, Object> responseMap = new HashMap<String, Object>();
			responseMap.putAll(map);
			for (Map.Entry<String, String> checkableDataType : changableDataType.entrySet()) {
				if (map.containsKey(checkableDataType.getKey())) {
					try {
						if ("STRING".equalsIgnoreCase(String.valueOf(checkableDataType.getValue()))) {
							responseMap.put(checkableDataType.getKey(), String.valueOf(map.get(checkableDataType.getKey())));
						} else if ("LONG".equalsIgnoreCase(String.valueOf(checkableDataType.getValue()))) {
							if (checkableDataType.getKey().equalsIgnoreCase("last_modified") || checkableDataType.getKey().equalsIgnoreCase("last_modified")) {
								Date date = new Date();
								String replacedvalue = map.get(checkableDataType.getKey()).toString().substring(0, 19);
								try {

									SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
									date = sf.parse(replacedvalue);
								} catch (Exception e) {

									SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
									date = sf.parse(replacedvalue);
								}
								responseMap.put(checkableDataType.getKey(), date.getTime());

							} else {
									try{
										responseMap.put(checkableDataType.getKey(), Long.valueOf((map.get(checkableDataType.getKey()).toString())));
									} catch(Exception e) {
										InsightsLogger.error("changeDataType exception", e);
									}							
									}
						} else if ("INTEGER".equalsIgnoreCase(String.valueOf(checkableDataType.getValue()))) {
							responseMap.put(checkableDataType.getKey(), Integer.valueOf(map.get(checkableDataType.getKey()).toString()));
						} else if ("DOUBLE".equalsIgnoreCase(String.valueOf(checkableDataType.getValue()))) {
							responseMap.put(checkableDataType.getKey(), Double.parseDouble(map.get(checkableDataType.getKey()).toString()));
						} else if ("BOOLEAN".equalsIgnoreCase(String.valueOf(checkableDataType.getValue()))) {
							responseMap.put(checkableDataType.getKey(), Boolean.valueOf(map.get(checkableDataType.getKey()).toString()));
						} else if ("OBJECT".equalsIgnoreCase(String.valueOf(checkableDataType.getValue()))) {
							responseMap.put(checkableDataType.getKey(), map.get(checkableDataType.getKey()));
						}
					} catch (Exception e) {
						InsightsLogger.error("changeDataType exception", e);
						responseMap.put(checkableDataType.getKey(), map.get(checkableDataType.getKey()));
					}
				}
			}
			changedList.add(responseMap);
		}
		return changedList;
	}

	public Date convertTimeZone(Date inputDate, String fromZone, String toZone) {
		boolean plus = false;
		boolean minus = false;
		Calendar cal = Calendar.getInstance(); // creates calendar
		cal.setTime(inputDate); // sets calendar time/date
		if (toZone.contains("plus")) {
			plus = true;
		} else if (toZone.contains("minus")) {
			minus = true;
		}
		String zone = toZone.replaceAll("plus", "");
		String DestintionZone = zone.replaceAll("minus", "");
		String[] dd = DestintionZone.split(":");
		if (plus) {
			for (int i = 0; i < dd.length; i++) {
				if (i == 0) {
					cal.add(Calendar.HOUR_OF_DAY, Integer.parseInt(dd[0])); // adds
																			// one
																			// hour
				} else if (i == 1) {
					cal.add(Calendar.MINUTE, Integer.parseInt(dd[1])); // adds
																		// one
																		// minute
				} else if (i == 2) {
					cal.add(Calendar.SECOND, Integer.parseInt(dd[1])); // adds
																		// one
																		// second

				}
			}
		} else if (minus) {
			for (int i = 0; i < dd.length; i++) {
				if (i == 0) {
					cal.add(Calendar.HOUR_OF_DAY, -Integer.parseInt(dd[0])); // adds
																				// one
																				// hour
				} else if (i == 1) {
					cal.add(Calendar.MINUTE, -Integer.parseInt(dd[1])); // adds
																		// one
																		// minute
				} else if (i == 2) {
					cal.add(Calendar.SECOND, -Integer.parseInt(dd[1])); // adds
																		// one
																		// second

				}
			}
		}
		return cal.getTime();
	}

	public Collection<String> generateYMWDKey(String dashboardKeys){
		
		Collection<String> returnDate = new ArrayList<String>();
		for(String value : dashboardKeys.split(",")){
		
			Date eventDateTime = new Date();
			String key = "";
			for(String splittedKey : value.split("~")){	
				String[]  subKey = null;
				if(splittedKey.startsWith("C:")){
					subKey = splittedKey.split(":");
					key += "~"+subKey[1];
				}else if(splittedKey.startsWith("D:")){
					subKey = splittedKey.split(":");
					dateFormatter = new SimpleDateFormat(subKey[1]);
					key += "~"+dateFormatter.format(eventDateTime).toString();
				}else{
	        		throw new RuntimeException("Invalide key format : "+splittedKey);
	        	}
			}
			returnDate.add(key.substring(1).trim());
		}
			return returnDate;
	}

public JSONObject mergeJSONObject(String raw,String custom,String arrayObjectIdentityfier){
	try {
	JSONObject  rawMap = new JSONObject(raw);
	JSONObject  customMap = new JSONObject(custom);
	Iterator<?> customkeys = customMap.keys();
	while(customkeys.hasNext()) {
		String customkey = String.valueOf(customkeys.next());
	try{
		if(customMap.get(customkey).getClass().getSimpleName().equals("JSONArray")){
					JSONArray customArray = (JSONArray) customMap.get(customkey);
					for(int i=0 ; i < customArray.length();i++){
						boolean flag = true;
						JSONArray updatedArray = new JSONArray();
						JSONArray rawArray = (JSONArray) rawMap.get(customkey);
						JSONObject  customObj = new JSONObject(customArray.get(i).toString());
						for(int j=0 ; j < rawArray.length();j++){
							JSONObject  rawObj = new JSONObject(rawArray.get(j).toString());
							
							if(customObj.get(arrayObjectIdentityfier).equals(rawObj.get(arrayObjectIdentityfier))){
								 Iterator<?> keys = customObj.keys();
								    while(keys.hasNext()) {
								    	flag = false;
								    	String key = String.valueOf(keys.next());
								        Object value = customObj.get(key);
								        rawObj.put(key, value);
								    }
							}
							updatedArray.put(rawObj);
					}
					if(flag){
						updatedArray.put(customObj);
					}
						rawMap.put(customkey, updatedArray);
				}
		}else if(customMap.get(customkey).getClass().getSimpleName().equals("JSONObject")){
			JSONObject  rawObj = new JSONObject(rawMap.get(customkey).toString());
			JSONObject  customObj = new JSONObject(customMap.get(customkey).toString());
			Iterator<?> keys = customObj.keys();
		    while(keys.hasNext()) {
		    	String key = String.valueOf(keys.next());
		        Object value = customObj.get(key);
		        rawObj.put(key, value);
		    }
		    rawMap.put(customkey, rawObj);
		}else{
				rawMap.put(customkey, customMap.get(customkey));
		}
		
		}catch(Exception e){
			InsightsLogger.error("mergeJSONObject exception", e);
			return null;
		}
	}
	return rawMap;
	} catch (Exception e) { 
		InsightsLogger.error("mergeJSONObject exception", e);
		return null;
	}
}
    public Map<String,String> getDisplayKey(String dashboardKeys){
        Map<String,String> returnDate = new LinkedHashMap<String, String>();
        SimpleDateFormat dateFormatter = null;
        if(dashboardKeys != null){
                for(String key : dashboardKeys.split(",")){
                        String rowKeys = "";
                        ColumnList<String> defination = cassandraService.getDashBoardKeys(key);
                        	String[] parts = key.split("~");
	            	        for(int i = 0 ; i < parts.length ; i++){
	            	        	if(parts[i].startsWith("D:")){
	            	        		String[] subKey = parts[i].split(":");
	            	    			dateFormatter = new SimpleDateFormat(subKey[1]);
	            	    			rowKeys += "~"+dateFormatter.format(new Date()).toString();
	            	        	}else if(parts[i].startsWith("C:")){
	            	        		String[] subKey = parts[i].split(":");
	            	        		rowKeys += "~"+subKey[1];
	            	        	}else{
	            	        		throw new RuntimeException("Invalide key format : "+ key);
	            	        	}
	            	        }	            	        
	            	        returnDate.put(rowKeys.substring(1).trim(), defination.getStringValue("constant_value", null));
	            	        
                        }
                }        
        return returnDate;
    }
    
    public Map<String,String> generateDiffYMWDValues(String dashboardKeys) throws ParseException{
		Map<String,String> returnDate = new LinkedHashMap<String, String>();
		if(dashboardKeys != null){
			for(String key : dashboardKeys.split(",")){
				String rowKey = "";
				String rowValue = "";
				String[] parts = key.split("~");
    	        for(int i = 0 ; i < parts.length ; i++){
    	        	if(parts[i].startsWith("D:")){
    	        		String[] subKey = parts[i].split(":");
    	    			dateFormatter = new SimpleDateFormat(subKey[1]);
    	    			String dateKeys = dateFormatter.format(new Date()).toString();
    	    			Date lastDate = dateFormatter.parse(dateKeys);
    					Date rowValues = new Date(lastDate.getTime() - 2);
    					rowKey += "~"+dateFormatter.format(lastDate);
    					rowValue += "~"+dateFormatter.format(rowValues);
    	        	}else if(parts[i].startsWith("C:")){
    	        		String[] subKey = parts[i].split(":");
    	        		rowKey += "~"+subKey[1];
    	        		rowValue += "~"+subKey[1];
    	        	}else{
    	        		throw new InsightsServerException("Invalide key format : "+ key);
    	        	}
    	        }	       
    	        returnDate.put(rowKey.substring(1).trim(), rowValue.substring(1).trim());
			}
		}
		return returnDate; 
	}
    
	@Override
	public String convertListToString(Collection<String> keyList) {
		
		StringBuilder sb = new StringBuilder();
		for (String key : keyList) {
			if(sb.length() > 0){
				sb.append(ApiConstants.COMMA);
			}
		    sb.append(key);
		}

		return sb.toString();
	}
	
	public Set<Object> convertArrayToSet(Object[] keyList){
		Set<Object> keySet = new HashSet<Object>();
			for(Object key : keyList){
				keySet.add(key);
			}
		return keySet;
	}
	
	
	public String generateTimeConversion(long mseconds) {
		long hours = 0;
		long minuts = 0;
		long seconds = 0;
		
		String getData = null;
		
		seconds = mseconds/1000;
		getData = String.valueOf(seconds+ " sec");
		minuts = seconds/60;
		if(minuts > 0 ){
			seconds = seconds - (minuts * 60);
		}
		hours = minuts/60;
		getData = String.valueOf(minuts + " min");
		if(hours > 0){
			minuts = minuts -(hours * 60);
		}
		
		if(hours == 0 && minuts > 0 && seconds > 0){
			getData = String.valueOf(minuts + " min " +seconds +" sec");
		}else if(hours == 0 && seconds == 0 && minuts > 0 ){
			getData = String.valueOf(minuts + " min");
			
		}else if(hours == 0 && minuts == 0 ){
			getData = String.valueOf(seconds + " sec");
		}else if(hours > 0 && minuts > 0){
			if(seconds >= 30){
				++ minuts;
			}
			if(minuts == 60){
				hours ++;
				minuts = minuts-60;
			}
			getData = String.valueOf(hours + " hrs " +minuts +" min");
		}else if(hours > 0 && minuts == 0 && seconds > 0){
			if(seconds >= 30){
				++ minuts;
				if(minuts == 60){
					++hours;
					minuts = minuts-60;
				}
				getData = String.valueOf(hours + " hrs " +minuts +" min");
			}else{
				getData = String.valueOf(hours + " hrs " +seconds +" sec");
			}
		}
		return getData;
	}
	
	   public String listMapToJsonString(List<Map<String, Object>> list){       
	        JSONObject jsonObj=new JSONObject();
	        for (Map<String, Object> map : list) {
	            JSONObject json_obj=new JSONObject();
	            for (Map.Entry<String, Object> entry : map.entrySet()) {
	                String key = entry.getKey();
	                Object value = entry.getValue();
	                try {
	                    json_obj.put(key,value);
	                } catch (JSONException e) {
	                	InsightsLogger.error("JSONException in listMapToJsonString", e);
	                }                           
	            }
	            try {
					jsonObj.append("metrics", json_obj);
				} catch (JSONException e) {
					InsightsLogger.error("JSONException in listMapToJsonString", e);
				}
	        }
	        return jsonObj.toString();
	    }
	   
		@Override
		public Map<String,Object> generateCurrentData(String defaultKeys,List<String> currentData) throws ParseException {
			
			Map<String,Object> mapData = new HashMap<String, Object>();
			for(int i=0;i<defaultKeys.split(",").length;i++) {
				mapData.put(defaultKeys.split(",")[i], currentData.get(i));
			}
			
			Map<String,Object> finalData = new LinkedHashMap<String, Object>();
			String key = null;
			SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
			SimpleDateFormat monthFormat = new SimpleDateFormat("yyyyMM");
			SimpleDateFormat weekFormat = new SimpleDateFormat("yyyyMMWW");
			SimpleDateFormat dayFormat = new SimpleDateFormat("yyyyMMdd");
			
			for(Map.Entry<String, Object> entry : mapData.entrySet()) {
				Calendar cal = Calendar.getInstance();
				key = entry.getKey();
				String[] checkKey = key.split(":");
				
				if(checkKey[1].equalsIgnoreCase("yyyy")) {
					cal.add(Calendar.YEAR, +1);
				} else if(checkKey[1].equalsIgnoreCase("yyyyMM")) {
					cal.add(Calendar.MONTH, +1);
				} else if(checkKey[1].equalsIgnoreCase("yyyyMMWW")) {
					cal.add(Calendar.WEEK_OF_MONTH, +1);
				} else if(checkKey[1].equalsIgnoreCase("yyyyMMdd")) {
					cal.add(Calendar.DAY_OF_YEAR, +1);
				}
				
				for(int i=0;i<7;i++) {
					if(checkKey[1].equalsIgnoreCase("yyyy")) {
						cal.add(Calendar.YEAR, -1);
						finalData.put(yearFormat.format(cal.getTime()), key);
					} else if(checkKey[1].equalsIgnoreCase("yyyyMM")) {
						cal.add(Calendar.MONTH, -1);
						finalData.put(monthFormat.format(cal.getTime()), key);
					} else if(checkKey[1].equalsIgnoreCase("yyyyMMWW")) {
						cal.add(Calendar.WEEK_OF_MONTH, -1);
						finalData.put(weekFormat.format(cal.getTime()), key);
					} else if(checkKey[1].equalsIgnoreCase("yyyyMMdd")) {
						cal.add(Calendar.DAY_OF_YEAR, -1);
						finalData.put(dayFormat.format(cal.getTime()), key);
					}
				}		
			}
			Map<String,Object> newFinalData = new TreeMap<String,Object>(finalData);
            return newFinalData;
		}
		
		public List<Map<String,Object>> appendInnerData(List<Map<String,Object>> requestData,Map<String,Map<Integer,String>> processMap,String key){

			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
			for(Map<String,Object> data : requestData){

				for(Entry<String, Map<Integer, String>> map : processMap.entrySet()){
					if(data.containsKey(map.getKey())){
						String mergedData=ApiConstants.STRING_EMPTY;
						boolean includeThumbnail = true;
						Map<Integer,String> appendableKey = new TreeMap<Integer, String>();
						appendableKey = map.getValue();
						
						for(Map.Entry<Integer, String> map2 : appendableKey.entrySet()){
							if(map2.getKey() == 0){
								mergedData=map2.getValue();	
								continue;
							}
							if(data.containsKey(map2.getValue()) && notNull(data.get(map2.getValue()).toString())){
								if(data.get(map2.getValue()).toString().startsWith(ApiConstants.HTTP)){
									mergedData = data.get(map2.getValue()).toString();
								}else{
								mergedData+=data.get(map2.getValue());
								}
							}else{
								includeThumbnail = false;
							}
						}
						if(includeThumbnail){
						data.put(map.getKey(), mergedData);
						}
					}
				}
				resultList.add(data);
			}
			return resultList;
		}
		
		public List<Map<String,Object>> replaceIfNull(List<Map<String,Object>> data,Map<String,String> replaceFields){
		
			List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
			for(Map<String,Object> map : data){
				Map<String,Object> resultMap = map;
				for(Map.Entry<String, String> replaceField : replaceFields.entrySet()){
					if(!map.containsKey(replaceField.getKey()) && map.containsKey(replaceField.getValue())){
						resultMap.put(replaceField.getKey(), map.get(replaceField.getValue()));	
					}
				}
				resultList.add(resultMap);
			}
			return resultList;
		}
		
		public List<Map<String, Object>> addCustomKeyInMapList(List<Map<String, Object>> rawRows, String columnName, String columnValue) {
			List<Map<String, Object>> rawRowsMapList = new ArrayList<Map<String, Object>>();
			for(Map<String, Object> rawRow : rawRows) {
				Map<String, Object> rawDataMap = new HashMap<String, Object>();
				rawDataMap.putAll(rawRow);
				rawDataMap.remove(ApiConstants.KEY);
				if(rawRow.containsKey(ApiConstants.KEY)) {
					rawDataMap.put(columnName, rawRow.get(ApiConstants.KEY));
				} else {
					rawDataMap.put(columnName, columnValue);
				}
				rawRowsMapList.add(rawDataMap);
			}
	 		return rawRowsMapList;
		}
		
	public String appendComma(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ApiConstants.COMMA);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
		
	public String appendTilda(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ApiConstants.TILDA);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
	
	public String appendForwardSlash(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ApiConstants.FORWARD_SLASH);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
		
		public String buildString(Object... texts){
			StringBuffer sb = new StringBuffer();
			for (Object text : texts) {
				sb.append(text);
			}
			return sb.toString();
		}
		
		public String errorHandler(String message, String... replace){
			
			for(int i=0; i<replace.length;i++){
				message = message.replace(ApiConstants.OPEN_BRACE+i+ApiConstants.CLOSE_BRACE, replace[i]);
			}
			return message;
		}

		public List<Map<String,Object>> convertMapToList(Map<String,?> requestMap,String key){
			List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
			for(String mapKey : requestMap.keySet()){
				Map<String,Object> dataMap = (Map<String, Object>) requestMap.get(mapKey);
				if(!dataMap.isEmpty()){
					dataMap.put(key, mapKey);
					dataList.add(dataMap);
				}
			}
			return dataList;
		}
		
		public List<Map<String,Object>> convertMapToList(Map<String,Object> requestMap,String key,String objectKey){
			List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
			for(String mapKey : requestMap.keySet()){
				Map<String,Object> dataMap = new HashMap<String,Object>();
				List<Map<String,Object>> tempList = (List<Map<String, Object>>)requestMap.get(mapKey);
				if(!tempList.isEmpty()){
					dataMap.put(key, mapKey);
					dataMap.put(objectKey, tempList);
					dataList.add(dataMap);
				}
			}
			return dataList;
		}
		
		public List<Map<String,Object>> groupRecordsBasedOnKey(List<Map<String,Object>> contentUsageList,String fetchKey,String objectKey){
			
			/**
			 * The Usage data is grouped depends on fetch key
			 */
			Map<String,Object> customizedMap = new HashMap<String,Object>();
			for(Map<String,Object> contentUsage : contentUsageList){
				if(contentUsage.get(fetchKey) != null){
					List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
					String key = contentUsage.get(fetchKey).toString();
					if(customizedMap.containsKey(key)){
						
						dataList.addAll((Collection<? extends Map<String, Object>>)customizedMap.get(key));
						contentUsage.remove(fetchKey);
						dataList.add(contentUsage);
					}else{
						contentUsage.remove(fetchKey);
						dataList.add(contentUsage);
					}
					customizedMap.put(key, dataList);
				}
			}
			/**
			 * The grouped data from fetch key is convered to list
			 */
			return convertMapToList(customizedMap,fetchKey,objectKey);
		}
		
		public String getHourlyBasedTimespent(double timeSpent) {

			long secs = Math.round(timeSpent / 1000);
			long hrs = (long) Math.floor(secs / 3600);
			long mins = (long) Math.floor((secs - (hrs * 3600)) / 60);
			long lsecs = (long) Math.floor(secs - (hrs * 3600) - (mins * 60));
			return ((hrs < 10) ? 0L + "" + hrs : hrs) + ":" + ((mins < 10) ? 0L + "" + mins : mins) + ":" + ((lsecs < 10) ? 0L + "" + lsecs : lsecs);
		}
}
