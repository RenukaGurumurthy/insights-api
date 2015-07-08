package org.gooru.insights.api.services;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.gooru.insights.api.models.RequestParamsDTO;
import org.json.JSONObject;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface BaseService {

	boolean notNull(String parameter);
	
	boolean notNull(Integer parameter);
	
	JSONObject validateJSON(String data) throws Exception;
	
	List<Map<String,Object>> changeDataType(String traceId, Map<String,String> changableDataType,List<Map<String,Object>> requestList);
	
	RequestParamsDTO buildRequestParameters(String data) throws Exception;
	
	List<Map<String,Object>> getColumnValues(String traceId, OperationResult<ColumnList<String>> columnList);
	
	Map<String,Object> getColumnValue(String traceId, OperationResult<ColumnList<String>> columnList);
	
	Map<String,Object> getLongValue(String traceId, OperationResult<ColumnList<String>> columnList);
	
	Map<String, Object> getRowLongValue(String traceId, OperationResult<Rows<String, String>> rowList);
	
	Map<String, Object> getRowLongValues(String traceId, OperationResult<Rows<String, String>> rowList);
	
	Map<String,String> getStringValue(OperationResult<ColumnList<String>> columnList);
	
	List<Map<String,Object>> getRowsColumnValues(String traceId, OperationResult<Rows<String, String>> rowList);
	
	List<Map<String,Object>> getData(List<Map<String,Object>> requestData,String coreKey);
	
	List<Map<String, Object>> LeftJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey);

	List<Map<String, Object>> rightJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey);

	List<Map<String, Object>> InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey);

	List<Map<String, Object>> InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String commonKey);

	Collection<String> appendAdditionalField(String selectFields,String prefix,String suffix);
	
	Collection<String> convertStringToCollection(String data);

	StringBuffer exportData(List<Map<String,Object>> requestData,String requestKey);
	
	Map<String,String> exportData(List<Map<String,Object>> requestData,Collection<String> requestKey);
	
	List<Map<String,Object>> JoinWithSingleKey(List<Map<String,Object>> multipleRecord,List<Map<String,Object>> singleRecord,String MultipleRecordKey);
	
	List<Map<String,Object>> properName(List<Map<String,Object>> requestList,Map<String,String> columnNames);
	
	List<Map<String,Object>> buildJSON(String traceId, List<Map<String,Object>> resultSet,Collection<String> additionParameter,Map<String,String> surName,boolean checkSession);

	List<Map<String,Object>> RandomJoin(List<Map<String,Object>> record1,List<Map<String,Object>> record2);

	Map<String,Object> getColumnValues(String traceId, OperationResult<ColumnList<String>> columnList,Map<String,Object> key);

	List<Map<String,Object>> getUserData(List<Map<String,Object>> requestData,String coreKey,Map<String,String> selectValue,String sortBy,String sortOrder,Integer limit);

	List<Map<String,Object>> getSingleKey(List<Map<String,Object>> singleRecord,String keyName,String RecordKey);

	List<Map<String,Object>> selfJoin(List<Map<String,Object>> requestData,String key,String appendField,String sortBy,String sotOrder,Integer limit);

	List<Map<String, Object>> safeJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey);
		
	List<Map<String,Object>> injectRecord(List<Map<String,Object>> aggregateData,Map<String,Object> injuctableRecord);
	
	List<Map<String, Object>> removeUnknownKeyList(List<Map<String, Object>> requestData,String key,String exceptionalkey,Object value,boolean status);

	List<Map<String, Object>> removeUnknownValueList(List<Map<String, Object>> requestData,String key,String validateValue,String exceptionalkey,Object value,boolean status);
		
	List<Map<String, Object>> combineTwoList(List<Map<String,Object>> parentList ,List<Map<String,Object>> childList,String parentKey,String childKey);
		
	List<Map<String, Object>> leftJoinwithTwoKey(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey);
	
	List<Map<String, Object>> sortBy(List<Map<String, Object>> requestData,String sortBy,String sortOrder);
	
	List<Map<String, Object>> injectCounterRecord(List<Map<String,Object>> aggregateData,Map<String,Object> injuctableRecord);

	Date convertTimeZone(Date inputDate, String fromZone, String toZone);
	
	 List<Map<String, Object>> InnerJoinContainsKey(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey,String childKey) ;
	 
	Collection<String> appendAdditionalField(String selectFields,String prefix,Collection<String> suffix);
	
	List<Map<String, Object>> properNameEndsWith(List<Map<String, Object>> requestList, Map<String, String> columnNames);
	
	Collection<String> generateYMWDKey(String keys);
	
	String convertListToString(Collection<String> keyList);
	
	Map<String,String> getDisplayKey(String traceId, String dashboardKeys);	
	
	String generateTimeConversion(long mseconds);

	Map<String,String> generateDiffYMWDValues(String traceId, String dashboardKeys) throws ParseException;
	
	String listMapToJsonString(String traceId,List<Map<String, Object>> list);
	
	boolean notNull(Map<?,?> request);

	Map<String,Object> generateCurrentData(String defaultKeys,List<String> currentData) throws ParseException;
	
	JSONObject mergeJSONObject(String traceId, String raw,String custom,String arrayObjectIdentityfier);
	
	List<Map<String,Object>> formatRecord(List<Map<String,Object>> rawData,Map<String,Object> injuctableRecord,String formatKey,String id);
	
	List<Map<String,Object>> appendInnerData(List<Map<String,Object>> requestData,Map<String,Map<Integer,String>> processMap,String key);
	
	List<Map<String,Object>> replaceIfNull(List<Map<String,Object>> data,Map<String,String> replaceField);
	
	List<Map<String, Object>> addCustomKeyInMapList(List<Map<String, Object>> rawRows, String columnName, String columnValue) ;
	
	void existsFilter(RequestParamsDTO requestParamsDTO)throws Exception;
	
	String appendComma(String... text);
	
	String appendTilda(String... texts);
	
	String errorHandler(String message, String... replace);
	
	List<Map<String,Object>> convertMapToList(Map<String,Map<String,Object>> requestMap,String key);
	
	List<Map<String,Object>> groupDataDependOnkey(List<Map<String,Object>> requestData,String fetchKey,String objectKey);
}
