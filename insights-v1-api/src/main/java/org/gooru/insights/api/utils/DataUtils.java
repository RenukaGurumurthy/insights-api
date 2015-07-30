package org.gooru.insights.api.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.options;
import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.models.InsightsConstant.ColumnFamily;
import org.gooru.insights.api.services.BaseService;
import org.gooru.insights.api.services.CassandraService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Component
public class DataUtils {

	private static Map<String,String> stringColumns;

	private static Map<String,String> longColumns;
	
	private static Set<String> studentsCollectionUsageColumnSuffix;
	
	private static Collection<String> collectionSummaryResourceColumns;

	private static Map<String,Map<String,String>> columnFamilyDataTypes;
	
	private static Map<String,String> resourceFields;
	
	@Autowired
	private BaseService baseService;
	
	@Autowired
	private CassandraService cassandraService;
	
	private static final Logger logger = LoggerFactory.getLogger(DataUtils.class);
	
	@PostConstruct
	private void init() {
		includeTableDataType();
		putResourceFields();
	}
	
	private void includeTableDataType(){
		if (columnFamilyDataTypes == null) {
			columnFamilyDataTypes = new HashMap<String, Map<String, String>>();
			ColumnFamily[] columnFamilies = ColumnFamily.values();
			Set<String> columnFamiliesName = new HashSet<String>();
			for (ColumnFamily columnFamily : columnFamilies) {
				columnFamiliesName.add(columnFamily.getColumnFamily());
			}
			OperationResult<Rows<String, String>> tableDataType = getCassandraService()
					.read(ApiConstants.BEAN_INIT,
							ColumnFamily.TABLE_DATATYPES.getColumnFamily(),
							columnFamiliesName);
			if (tableDataType != null && !tableDataType.getResult().isEmpty()) {
				for (Row<String, String> row : tableDataType.getResult()) {
					Map<String, String> dataType = new HashMap<String, String>();
					for (Column<String> column : row.getColumns()) {
						dataType.put(column.getName(), column.getStringValue());
					}
					columnFamilyDataTypes.put(row.getKey(), dataType);
				}
			}
		}
	}
	
	private void putResourceFields(){
		
		resourceFields = new HashMap<String,String>();
		resourceFields.put(ApiConstants.TITLE,ApiConstants.TITLE);
		resourceFields.put(ApiConstants.RESOURCE_TYPE,ApiConstants.TYPE);
		resourceFields.put(ApiConstants.THUMBNAIL,ApiConstants.THUMBNAIL);
		resourceFields.put(ApiConstants.GOORUOID,ApiConstants.GOORUOID);
		resourceFields.put(ApiConstants._GOORUOID,ApiConstants.GOORUOID);
	}
	
	public static Set<Object> convertArrayToSet(Object[] keyList){
		Set<Object> keySet = new HashSet<Object>();
			for(Object key : keyList){
				keySet.add(key);
			}
		return keySet;
	}
	
	static{
		putStudentCollectionUsageCache();
		putCollectionSummaryUsageCache();
	}

	private static void putStudentCollectionUsageCache(){
		putStudentCollectionColumnSuffix();
	}
	
	private static void putCollectionSummaryUsageCache(){
		putcollectionSummaryResourceColumn();
	}
	
	private static void putcollectionSummaryResourceColumn(){
		collectionSummaryResourceColumns = new ArrayList<String>();
		collectionSummaryResourceColumns.add(ApiConstants.TITLE);
		collectionSummaryResourceColumns.add(ApiConstants.THUMBNAIL);
		collectionSummaryResourceColumns.add(ApiConstants.GOORUOID);
		collectionSummaryResourceColumns.add(ApiConstants.RESOURCE_FORMAT);
		collectionSummaryResourceColumns.add(ApiConstants.RESOURCE_TYPE);
		collectionSummaryResourceColumns.add(ApiConstants.CATEGORY);
		collectionSummaryResourceColumns.add(ApiConstants.HAS_FRAME_BREAKER);
		collectionSummaryResourceColumns.add(ApiConstants.QUESTION_DOT_TYPE);
		collectionSummaryResourceColumns.add(ApiConstants.QUESTION_DOT_QUESTION_TYPE);
	}
	
	private static void putStudentCollectionColumnSuffix(){
		studentsCollectionUsageColumnSuffix = new HashSet<String>();
		studentsCollectionUsageColumnSuffix.add(ApiConstants.VIEWS);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._TIME_SPENT);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._QUESTION_STATUS);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._ANSWER_OBJECT);
		studentsCollectionUsageColumnSuffix.add(ApiConstants.CHOICE);
		studentsCollectionUsageColumnSuffix.add(ApiConstants.ATTEMPTS);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._AVG_TIME_SPENT);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._TIME_SPENT);
		studentsCollectionUsageColumnSuffix.add(ApiConstants.OPTIONS);
		studentsCollectionUsageColumnSuffix.add(options.A.name());
		studentsCollectionUsageColumnSuffix.add(options.B.name());
		studentsCollectionUsageColumnSuffix.add(options.C.name());
		studentsCollectionUsageColumnSuffix.add(options.D.name());
		studentsCollectionUsageColumnSuffix.add(options.E.name());
		studentsCollectionUsageColumnSuffix.add(options.F.name());
	}
	
	static{
		longColumns = new HashMap<String, String>();
		longColumns.put(ApiConstants.VIEWS, ApiConstants.VIEWS);
		longColumns.put(ApiConstants._SCORE_IN_PERCENTAGE, ApiConstants.SCORE_IN_PERCENTAGE);
		longColumns.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		longColumns.put(ApiConstants.REACTION, ApiConstants.REACTION);
		longColumns.put(ApiConstants.SCORE, ApiConstants.SCORE);
		longColumns.put(ApiConstants.SKIPPED, ApiConstants.SKIPPED);
		longColumns.put("attempts", "attempts");
		longColumns.put("correct", "totalCorrectCount");
		longColumns.put("in_correct", "totalInCorrectCount");
		longColumns.put("A", "options");
		longColumns.put("B", "options");
		longColumns.put("C", "options");
		longColumns.put("D", "options");
		longColumns.put("E", "options");
		longColumns.put("F", "options");
	}
	
	static{
		stringColumns = new HashMap<String, String>();
		stringColumns.put(ApiConstants.CHOICE, ApiConstants.TEXT);
		stringColumns.put(ApiConstants._FEEDBACK_PROVIDER, ApiConstants.FEEDBACKPROVIDER);
		stringColumns.put(ApiConstants._QUESTION_STATUS, ApiConstants.STATUS);
		stringColumns.put("answer_object", "answerObject");
	}

	private static Map<String,String> sessionActivityMetrics;

	static {
		sessionActivityMetrics = new HashMap<String, String>();
		sessionActivityMetrics.put(ApiConstants._COLLECTION_TYPE, ApiConstants.COLLECTION_TYPE);
		sessionActivityMetrics.put(ApiConstants.VIEWS, ApiConstants.VIEWS);
		sessionActivityMetrics.put(ApiConstants._SCORE_IN_PERCENTAGE, ApiConstants.SCORE_IN_PERCENTAGE);
		sessionActivityMetrics.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		sessionActivityMetrics.put(ApiConstants.REACTION, ApiConstants.REACTION);
		sessionActivityMetrics.put(ApiConstants.CHOICE, ApiConstants.TEXT);
		sessionActivityMetrics.put(ApiConstants.TYPE, ApiConstants.QUESTION_TYPE);
		sessionActivityMetrics.put(ApiConstants._FEEDBACK_PROVIDER, ApiConstants.FEEDBACKPROVIDER);
		sessionActivityMetrics.put(ApiConstants._QUESTION_STATUS, ApiConstants.STATUS);
		sessionActivityMetrics.put(ApiConstants.SCORE, ApiConstants.SCORE);
		sessionActivityMetrics.put(ApiConstants.TAU, ApiConstants.TOTAL_ATTEMPT_USER_COUNT);
		sessionActivityMetrics.put(ApiConstants.SKIPPED, ApiConstants.SKIPPED);
		sessionActivityMetrics.put(ApiConstants.ATTEMPTS, ApiConstants.ATTEMPTS);
		sessionActivityMetrics.put(ApiConstants.CORRECT, ApiConstants.TOTAL_CORRECT_COUNT);
		sessionActivityMetrics.put(ApiConstants.IN_CORRECT, ApiConstants.TOTAL_INCORRECT_COUNT);
		sessionActivityMetrics.put(ApiConstants._ANSWER_OBJECT,ApiConstants.ANSWER_OBJECT);
		sessionActivityMetrics.put(ApiConstants.OPTIONS, ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.A.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.B.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.C.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.D.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.E.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.F.option(), ApiConstants.OPTIONS);
	}
	
	public static String getSessionActivityMetricsMapValue(String key) {
		return sessionActivityMetrics.containsKey(key) ? sessionActivityMetrics.get(key) : null;
	}
	
	public static Map<String, String> getSessionActivityMetricsMap() {
		return sessionActivityMetrics;
	}

	public static Map<String,String> getStringColumns() {
		return stringColumns;
	}
	public static Map<String,String> getLongColumns() {
		return longColumns;
	}

	public static Set<String> getStudentsCollectionUsageColumnSuffix() {
		return studentsCollectionUsageColumnSuffix;
	}

	public static void setStudentsCollectionUsageColumnSuffix(
			Set<String> studentsCollectionUsageColumnSuffix) {
		DataUtils.studentsCollectionUsageColumnSuffix = studentsCollectionUsageColumnSuffix;
	}

	public static Collection<String> getCollectionSummaryResourceColumns() {
		return collectionSummaryResourceColumns;
	}

	public static void setCollectionSummaryResourceColumns(
			Collection<String> collectionSummaryResourceColumns) {
		DataUtils.collectionSummaryResourceColumns = collectionSummaryResourceColumns;
	}
	
	public static Map<String,Object> getColumnFamilyContent(String traceId, String columnFamily, Row<String, String> row, Map<String,String> aliesNames, Collection<String> columnNames, Collection<String> conditionalColumns){
	
		Map<String,String> dataTypes = getColumnFamilyDataTypes().get(columnFamily);
		Map<String,Object> dataMap = new HashMap<String,Object>();
		Collection<String> columnList = new ArrayList<String>(columnNames);
		if(conditionalColumns != null){
			for(String conditionalColumn : conditionalColumns){
				String[] dependentColumn = conditionalColumn.split(ApiConstants.COMMA);
				for(int i =0; i<dependentColumn.length; i++){
					if(columnList.contains(dependentColumn[i])){
						String apiField = aliesNames.get(dependentColumn[i]) != null ? aliesNames.get(dependentColumn[i]) : dependentColumn[i];
						fetchData(traceId, columnFamily, dataTypes, dependentColumn[i], apiField, row, dataMap);
						if(dataMap.get(apiField) != null){
							for(String column : dependentColumn){
								columnList.remove(column);
							}
							break;
						}
					}
				}
			}
		}
		for(String columnName : columnList){
			String apiField = aliesNames.get(columnName) != null ? aliesNames.get(columnName) : columnName;
			fetchData(traceId, columnFamily, dataTypes, columnName, apiField, row, dataMap);
		}
		return dataMap;
	}
	
	public static Map<String,Object> getColumnFamilyContent(String traceId, String columnFamily, ColumnList<String> columns, Map<String,String> aliesNames, Collection<String> columnNames, Collection<String> conditionalColumns){
		
		Map<String,String> dataTypes = getColumnFamilyDataTypes().get(columnFamily);
		Map<String,Object> dataMap = new HashMap<String,Object>();
		Collection<String> columnList = new ArrayList<String>(columnNames);
		if(conditionalColumns != null){
			for(String conditionalColumn : conditionalColumns){
				String[] dependentColumn = conditionalColumn.split(ApiConstants.PIPE);
				for(int i =0; i<dependentColumn.length; i++){
					if(columnList.contains(dependentColumn[i])){
						String apiField = aliesNames.get(dependentColumn[i]) != null ? aliesNames.get(dependentColumn[i]) : dependentColumn[i];
						fetchData(traceId, columnFamily, dataTypes, dependentColumn[i], apiField, columns, dataMap);
						if(dataMap.get(apiField) != null){
							columnList.remove(dependentColumn);
							break;
						}
					}
				}
			}
		}
		for(String columnName : columnList){
			String apiField = aliesNames.get(columnName) != null ? aliesNames.get(columnName) : columnName;
			fetchData(traceId, columnFamily, dataTypes, columnName, apiField, columns, dataMap);
		}
		return dataMap;
	}
	private static void fetchData(String traceId, String columnFamily, Map<String,String> dataTypes, String columnName, String apiField, ColumnList<String> columns, Map<String,Object> dataMap){
		if(dataTypes.get(columnName) != null){
			if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.STRING.dataType())){
				dataMap.put(apiField, columns.getStringValue(columnName, null));
			}else if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.INT.dataType())){
				dataMap.put(apiField, columns.getIntegerValue(columnName, 0));
			}else if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.LONG.dataType())){
				dataMap.put(apiField, columns.getLongValue(columnName, 0L));
			}else if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.DATE.dataType())){
				dataMap.put(apiField, columns.getDateValue(columnName, null));
			}else{
				dataMap.put(apiField, columns.getStringValue(columnName, null));
				InsightsLogger.debug(traceId, buildMessage(ErrorMessages.UNHANDLED_FIELD,columnFamily,columnName));
			}
		}else {
			InsightsLogger.debug(traceId, buildMessage(ErrorMessages.UNHANDLED_FIELD,columnFamily,columnName));
		}
	}

	private static void fetchData(String traceId, String columnFamily, Map<String,String> dataTypes, String columnName, String apiField, Row<String, String> row, Map<String,Object> dataMap){
		if(dataTypes.get(columnName) != null){
			if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.STRING.dataType())){
				dataMap.put(apiField, row.getColumns().getStringValue(columnName, null));
			}else if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.INT.dataType())){
				dataMap.put(apiField, row.getColumns().getIntegerValue(columnName, 0));
			}else if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.LONG.dataType())){
				dataMap.put(apiField, row.getColumns().getLongValue(columnName, 0L));
			}else if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.DATE.dataType())){
				dataMap.put(apiField, row.getColumns().getDateValue(columnName, null));
			}else{
				dataMap.put(apiField, row.getColumns().getStringValue(columnName, null));
				InsightsLogger.debug(traceId, buildMessage(ErrorMessages.UNHANDLED_FIELD,columnFamily,columnName));
			}
		}else {
			dataMap.put(apiField, row.getColumns().getStringValue(columnName, null));
			InsightsLogger.debug(traceId, buildMessage(ErrorMessages.UNHANDLED_FIELD,columnFamily,columnName));
		}
	}
	
	public static String buildMessage(String message, String... replacer){
		
		for(int count =0;count<replacer.length;count++){
			message = message.replace(ApiConstants.OPEN_BRACE+count+ApiConstants.CLOSE_BRACE, replacer[count]);
		}
		return message;
	}
	
	public static String getTimeDifference(String traceId,Date lastModified) {
		String lagTime = null;
		try {
			Date currentTime = new Date();
			long lagInMilliSecs = (currentTime.getTime() - lastModified.getTime());
			lagTime = String.format("%02d", TimeUnit.MILLISECONDS.toSeconds(lagInMilliSecs));
		} catch (Exception e2) {
			logger.error("Exception : "+e2);
			throw new InternalError(e2.getMessage());
		}
		return lagTime;

	}
	
	public BaseService getBaseService() {
		return baseService;
	}

	public void setBaseService(BaseService baseService) {
		this.baseService = baseService;
	}
	public CassandraService getCassandraService() {
		return cassandraService;
	}
	public void setCassandraService(CassandraService cassandraService) {
		this.cassandraService = cassandraService;
	}

	public static Map<String, Map<String, String>> getColumnFamilyDataTypes() {
		return columnFamilyDataTypes;
	}

	public static Map<String, String> getResourceFields() {
		return resourceFields;
	}
	
}
