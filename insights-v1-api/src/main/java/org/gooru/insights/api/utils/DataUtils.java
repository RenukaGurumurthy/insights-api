package org.gooru.insights.api.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.options;
import org.gooru.insights.api.constants.InsightsConstant.ColumnFamilySet;
import org.gooru.insights.api.constants.ErrorMessages;
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

	private static String nfsLocation;
	
	private static Map<String,String> stringColumns;

	private static Map<String,String> longColumns;
	
	private static Set<String> studentsCollectionUsageColumnSuffix;
	
	private static Map<String, String> studentsCollectionUsage;
	
	private static Collection<String> collectionSummaryResourceColumns;

	private static Map<String,String> unitProgressActivityFields;

	private static Map<String,String> resourceFields;
	
	private static Map<String,String> lessonPlanClassActivityFields;
	
	private static Map<String,Map<String,String>> ColumnFamilySetDataTypes;

	private static Map<String,Map<String,List<String>>> mergeDualColumnValues;
	
 	private static Map<String,String> allStudentUnitProgress;
 	
	@Autowired
	private BaseService baseService;
	
	@Autowired
	private CassandraService cassandraService;
	
	@Resource
	private Properties filePath;

	private static final Logger logger = LoggerFactory.getLogger(DataUtils.class);

	@PostConstruct
	private void init() {
		includeTableDataType();
		putResourceFields();
		putMergeDualColumnValues();
		putNFSLocation();
		putLessonPlanClassActivityFields();
		putUnitProgressActivityFields();
		putallStudentUnitProgress();
		putStudentsCollectionUsage();
	}
	
	private void putNFSLocation(){
		nfsLocation = filePath.getProperty(ApiConstants.NFS_BUCKET);
	}
	
	private void includeTableDataType(){
		if (ColumnFamilySetDataTypes == null) {
			ColumnFamilySetDataTypes = new HashMap<String, Map<String, String>>();
			ColumnFamilySet[] columnFamilies = ColumnFamilySet.values();
			Set<String> columnFamiliesName = new HashSet<String>();
			for (ColumnFamilySet ColumnFamilySet : columnFamilies) {
				columnFamiliesName.add(ColumnFamilySet.getColumnFamily());
			}
			OperationResult<Rows<String, String>> tableDataType = getCassandraService()
					.read(ColumnFamilySet.TABLE_DATATYPES.getColumnFamily(),
							columnFamiliesName);
			if (tableDataType != null && !tableDataType.getResult().isEmpty()) {
				for (Row<String, String> row : tableDataType.getResult()) {
					Map<String, String> dataType = new HashMap<String, String>();
					for (Column<String> column : row.getColumns()) {
						dataType.put(column.getName(), column.getStringValue());
					}
					ColumnFamilySetDataTypes.put(row.getKey(), dataType);
				}
			}
		}
	}
	
	private void putResourceFields(){
		
		resourceFields = new HashMap<String,String>();
		resourceFields.put(ApiConstants.TITLE,ApiConstants.TITLE);
		resourceFields.put(ApiConstants.RESOURCE_TYPE,ApiConstants.TYPE);
		resourceFields.put(ApiConstants.THUMBNAIL,ApiConstants.THUMBNAIL);
		resourceFields.put(ApiConstants.FOLDER,ApiConstants.FOLDER);
		resourceFields.put(ApiConstants.GOORUOID,ApiConstants.GOORUOID);
		resourceFields.put(ApiConstants._GOORUOID,ApiConstants.GOORUOID);
	}
	
	private void putallStudentUnitProgress() {
		allStudentUnitProgress = new HashMap<String,String>();
		allStudentUnitProgress.put(ApiConstants._SCORE_IN_PERCENTAGE, ApiConstants.SCORE_IN_PERCENTAGE);
		allStudentUnitProgress.put(ApiConstants._ASSESSMENT_UNIQUE_VIEWS, ApiConstants.VIEWS);
	}
	
	private void putLessonPlanClassActivityFields(){
		lessonPlanClassActivityFields = new HashMap<String,String>();
		lessonPlanClassActivityFields.put(ApiConstants.VIEWS, ApiConstants.VIEWS);
		lessonPlanClassActivityFields.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		lessonPlanClassActivityFields.put(ApiConstants._SCORE_IN_PERCENTAGE, ApiConstants.SCORE_IN_PERCENTAGE);
		lessonPlanClassActivityFields.put(ApiConstants._COLLECTION_TYPE, ApiConstants.TYPE);
		lessonPlanClassActivityFields.put(ApiConstants._LAST_ACCESSED, ApiConstants.LAST_ACCESSED);
		lessonPlanClassActivityFields.put(ApiConstants.EVIDENCE, ApiConstants.EVIDENCE);
	}
	
	private void putMergeDualColumnValues(){
		
		mergeDualColumnValues = new HashMap<String,Map<String,List<String>>>();
		putResourceMergeConfig(mergeDualColumnValues);
	}
	
	private void putUnitProgressActivityFields() {
		
		unitProgressActivityFields = new HashMap<String,String>();
		unitProgressActivityFields.put(ApiConstants.VIEWS, ApiConstants.VIEWS);
		unitProgressActivityFields.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		unitProgressActivityFields.put(ApiConstants._SCORE_IN_PERCENTAGE, ApiConstants.SCORE_IN_PERCENTAGE);
	}
	
	private void putResourceMergeConfig(Map<String,Map<String,List<String>>> mergeDualColumnValues){

		Map<String,List<String>> dependentColumn = new HashMap<String,List<String>>();
		dependentColumn.put(getBaseService().appendComma(ApiConstants.GOORUOID,ApiConstants._GOORUOID), ServiceUtils.generateList(ApiConstants.GOORUOID,ApiConstants._GOORUOID));
		dependentColumn.put(getBaseService().appendComma(ApiConstants.QUESTION_DOT_QUESTION_TYPE,ApiConstants.QUESTION_DOT_TYPE), ServiceUtils.generateList(ApiConstants.QUESTION_DOT_QUESTION_TYPE,ApiConstants.QUESTION_DOT_TYPE));
		mergeDualColumnValues.put(ColumnFamilySet.RESOURCE.getColumnFamily(), dependentColumn);
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
	
	private static void putStudentsCollectionUsage() {
		
		studentsCollectionUsage = new HashMap<String, String>();
		studentsCollectionUsage.put(ApiConstants.VIEWS, ApiConstants.VIEWS);
		studentsCollectionUsage.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		studentsCollectionUsage.put(ApiConstants._ANSWER_OBJECT, ApiConstants.ANSWER_OBJECT);
		studentsCollectionUsage.put(ApiConstants.CHOICE, ApiConstants.TEXT);
		studentsCollectionUsage.put(ApiConstants.ATTEMPTS, ApiConstants.ATTEMPTS);
		studentsCollectionUsage.put(ApiConstants._AVG_TIME_SPENT, ApiConstants.AVG_TIME_SPENT);
		studentsCollectionUsage.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		studentsCollectionUsage.put(ApiConstants.options.A.name(), ApiConstants.OPTIONS);
		studentsCollectionUsage.put(ApiConstants.options.B.name(), ApiConstants.OPTIONS);
		studentsCollectionUsage.put(ApiConstants.options.C.name(), ApiConstants.OPTIONS);
		studentsCollectionUsage.put(ApiConstants.options.D.name(), ApiConstants.OPTIONS);
		studentsCollectionUsage.put(ApiConstants.options.E.name(), ApiConstants.OPTIONS);
		studentsCollectionUsage.put(ApiConstants.options.F.name(), ApiConstants.OPTIONS);
		studentsCollectionUsage.put(ApiConstants._QUESTION_STATUS, ApiConstants.STATUS);
		studentsCollectionUsage.put(ApiConstants.OPTIONS, ApiConstants.OPTIONS);
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
		
	}
	
	static{
		stringColumns = new HashMap<String, String>();
		stringColumns.put(ApiConstants.CHOICE, ApiConstants.TEXT);
		stringColumns.put(ApiConstants._FEEDBACK_PROVIDER, ApiConstants.FEEDBACKPROVIDER);
		stringColumns.put(ApiConstants._QUESTION_STATUS, ApiConstants.STATUS);
		stringColumns.put(ApiConstants.OPTIONS, ApiConstants.OPTIONS);
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
	
	public static Map<String,Object> getColumnFamilySetContent(String ColumnFamilySet, ColumnList<String> columnList, Map<String,String> aliesNames, String key, Collection<String> columnNames, Map<String,List<String>> mergeResourceDualColumnValues, boolean isSecure){
	
		Map<String,String> dataTypes = getColumnFamilySetDataTypes().get(ColumnFamilySet);
		Map<String,Object> dataMap = new HashMap<String,Object>();
		Collection<String> RequestedColumns = new ArrayList<String>(columnNames);
		if(RequestedColumns.contains(ApiConstants.THUMBNAIL)){
			buildThumbnailURL(columnList,isSecure, dataMap);
			RequestedColumns.remove(ApiConstants.THUMBNAIL);
			RequestedColumns.remove(ApiConstants.FOLDER);
		}
		if(mergeResourceDualColumnValues != null){
			handleMergeColumnValues(ColumnFamilySet, columnList, RequestedColumns, dataMap, dataTypes, aliesNames, mergeResourceDualColumnValues);
		}
		for(String columnName : RequestedColumns){
			String apiField = aliesNames.get(columnName) != null ? aliesNames.get(columnName) : columnName;
			fetchData(ColumnFamilySet, dataTypes, key, columnName, apiField, columnList, dataMap);
		}
		return dataMap;
	}
	
	private static void buildThumbnailURL(ColumnList<String> columns, boolean isSecure, Map<String,Object> resourceMetaData){
		
		String thumbnail = null;
		thumbnail = columns.getStringValue(ApiConstants.THUMBNAIL, null);
		if(thumbnail != null && !thumbnail.startsWith(ApiConstants.HTTP)) {
			String path = columns.getStringValue(ApiConstants.FOLDER, ApiConstants.STRING_EMPTY);
			path = ServiceUtils.appendForwardSlash(nfsLocation, thumbnail.contains(path) ? null : path, thumbnail);
			thumbnail = ServiceUtils.buildString(isSecure ? ApiConstants._HTTPS : ApiConstants._HTTP,path);
		}
		resourceMetaData.put(ApiConstants.THUMBNAIL, thumbnail);
	}
	
	private static void handleMergeColumnValues(String ColumnFamilySet,
			ColumnList<String> columnList, Collection<String> RequestedColumns,
			Map<String, Object> dataMap, Map<String, String> dataTypes,
			Map<String, String> aliesNames,
			Map<String, List<String>> mergeResourceDualColumnValues) {

		for (Map.Entry<String, List<String>> mergeColumns : mergeResourceDualColumnValues.entrySet()) {
			if (RequestedColumns.containsAll(mergeColumns.getValue())) {
				for (String column : mergeColumns.getValue()) {
					String apiField = aliesNames.get(column) != null ? aliesNames
							.get(column) : column;
					fetchData(ColumnFamilySet, dataTypes, null, column,
							apiField, columnList, dataMap);
					if (dataMap.get(apiField) != null) {
						RequestedColumns.removeAll(mergeColumns.getValue());
						break;
					}
				}
			}
		}
	}
	
	public static void fetchData(String ColumnFamilySet, Map<String,String> dataTypes,String columnPrefix, String columnName, String apiField, ColumnList<String> columns, Map<String,Object> dataMap){

		String fetchColumnName = columnName;
		if(columnPrefix != null){
			fetchColumnName = columnPrefix+ApiConstants.TILDA+columnName;
		}
		if(!customFetchData(columnPrefix, columnName, dataMap, columns)) {
			if(dataTypes.get(columnName) != null){
				if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.STRING.dataType()) || dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.TEXT.dataType())){
					dataMap.put(apiField, columns.getStringValue(fetchColumnName, null));
				}else if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.INT.dataType())){
					dataMap.put(apiField, columns.getIntegerValue(fetchColumnName, 0));
				}else if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.LONG.dataType())){
					dataMap.put(apiField, columns.getLongValue(fetchColumnName, 0L));
				}else if(dataTypes.get(columnName).equalsIgnoreCase(ApiConstants.dataTypes.DATE.dataType())){
					dataMap.put(apiField, columns.getDateValue(fetchColumnName, null));
				}else{
					dataMap.put(apiField, columns.getStringValue(fetchColumnName, null));
					InsightsLogger.debug(buildMessage(ErrorMessages.UNHANDLED_FIELD,ColumnFamilySet,columnName));
				}
			}else {
				dataMap.put(apiField, columns.getStringValue(fetchColumnName, null));
				InsightsLogger.debug(buildMessage(ErrorMessages.UNHANDLED_FIELD,ColumnFamilySet,columnName));
			}
		}
	}
	
	private static boolean customFetchData(String columnPrefix, String columnName, Map<String,Object> dataMap, ColumnList<String> columns) {
	
		boolean processed = true;
			if(columnName.equals(ApiConstants.OPTIONS)) {
				Map<String,Long> optionsMap = new HashMap<String,Long>();
				optionsMap.put(ApiConstants.options.A.name(), columns.getLongValue(ServiceUtils.appendTilda(columnPrefix,ApiConstants.options.A.name()), 0L));
				optionsMap.put(ApiConstants.options.B.name(), columns.getLongValue(ServiceUtils.appendTilda(columnPrefix,ApiConstants.options.B.name()), 0L));
				optionsMap.put(ApiConstants.options.C.name(), columns.getLongValue(ServiceUtils.appendTilda(columnPrefix,ApiConstants.options.C.name()), 0L));
				optionsMap.put(ApiConstants.options.D.name(), columns.getLongValue(ServiceUtils.appendTilda(columnPrefix,ApiConstants.options.D.name()), 0L));
				optionsMap.put(ApiConstants.options.E.name(), columns.getLongValue(ServiceUtils.appendTilda(columnPrefix,ApiConstants.options.E.name()), 0L));
				optionsMap.put(ApiConstants.options.F.name(), columns.getLongValue(ServiceUtils.appendTilda(columnPrefix,ApiConstants.options.F.name()), 0L));
				dataMap.put(ApiConstants.OPTIONS, optionsMap);
			} else if(columnName.equals(ApiConstants._QUESTION_STATUS)) {
				String responseStatus = columns.getStringValue(ServiceUtils.appendTilda(columnPrefix,columnName), null);
				dataMap.put(ApiConstants.STATUS, responseStatus);
				dataMap.put(ApiConstants.SCORE, (responseStatus != null && responseStatus.equalsIgnoreCase(ApiConstants.CORRECT)) ? 1L : 0L);
			} else {
				processed = false;
			}
		return processed;
	}
	
	private static boolean customFetchDefaultData(String columnName, Map<String,Object> dataMap) {
		
		boolean processed = true;
			if(columnName.equals(ApiConstants.OPTIONS)) {
				Map<String,Long> optionsMap = new HashMap<String,Long>();
				optionsMap.put(ApiConstants.options.A.name(), 0L);
				optionsMap.put(ApiConstants.options.B.name(), 0L);
				optionsMap.put(ApiConstants.options.C.name(), 0L);
				optionsMap.put(ApiConstants.options.D.name(), 0L);
				optionsMap.put(ApiConstants.options.E.name(), 0L);
				optionsMap.put(ApiConstants.options.F.name(), 0L);
				dataMap.put(ApiConstants.OPTIONS, optionsMap);
			} else if(columnName.equals(ApiConstants._QUESTION_STATUS)) {
				dataMap.put(ApiConstants.STATUS, null);
				dataMap.put(ApiConstants.SCORE,  0L);
			} else {
				processed = false;
			}
		return processed;
	}
	
	public static void fetchDefaultData(String ColumnFamilySet, Map<String,String> aliesNames, Map<String,Object> dataMap){
		
		Map<String,String> dataTypes = getColumnFamilySetDataTypes().get(ColumnFamilySet);
		
			if(dataTypes != null) {
				for(Entry<String, String> aliesName : aliesNames.entrySet()){
					if(dataTypes.get(aliesName.getKey()) != null){
						if(customFetchDefaultData(aliesName.getKey(), dataMap)) {
							continue;
						}
						if(dataTypes.get(aliesName.getKey()).equalsIgnoreCase(ApiConstants.dataTypes.STRING.dataType()) || dataTypes.get(aliesName.getKey()).equalsIgnoreCase(ApiConstants.dataTypes.TEXT.dataType())){
							dataMap.put(aliesName.getValue(), null);
						}else if(dataTypes.get(aliesName.getKey()).equalsIgnoreCase(ApiConstants.dataTypes.INT.dataType())){
							dataMap.put(aliesName.getValue(), 0);
						}else if(dataTypes.get(aliesName.getKey()).equalsIgnoreCase(ApiConstants.dataTypes.LONG.dataType())){
							dataMap.put(aliesName.getValue(), 0L);
						}else if(dataTypes.get(aliesName.getKey()).equalsIgnoreCase(ApiConstants.dataTypes.DATE.dataType())){
							dataMap.put(aliesName.getValue(), null);
						}else{
							dataMap.put(aliesName.getValue(), null);
							InsightsLogger.debug(buildMessage(ErrorMessages.UNHANDLED_FIELD,ColumnFamilySet,aliesName.getValue()));
						}
					}else {
						dataMap.put(aliesName.getValue(), null);
						InsightsLogger.debug(buildMessage(ErrorMessages.UNHANDLED_FIELD,ColumnFamilySet,aliesName.getValue()));
					}
				}
			}
	}
	
	public static String buildMessage(String message, String... replacer){
		
		for(int count =0;count<replacer.length;count++){
			message = message.replace(ApiConstants.OPEN_BRACE+count+ApiConstants.CLOSE_BRACE, replacer[count]);
		}
		return message;
	}
	
	public static String getTimeDifference(Date lastModified) {
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

	public static Map<String, Map<String, String>> getColumnFamilySetDataTypes() {
		return ColumnFamilySetDataTypes;
	}

	public static Map<String, String> getResourceFields() {
		return resourceFields;
	}
	
	public static Map<String, String>  getLessonPlanClassActivityFields() {
		return lessonPlanClassActivityFields;
	}
	
	public static Map<String, String>  getUnitProgressActivityFields() {
		return unitProgressActivityFields;
	}

	public static Map<String, Map<String,List<String>>> getMergeDualColumnValues() {
		return mergeDualColumnValues;
	}
	
	public static Map<String,String> getAllStudentUnitProgress() {
		return allStudentUnitProgress;
	}
	
	public static Map<String, String> getStudentsCollectionUsage() {
		return studentsCollectionUsage;
	}
}
