package org.gooru.insights.api.services;

import java.io.IOException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.addQuestionType;
import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.RequestParamsDTO;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.spring.exception.BadRequestException;
import org.gooru.insights.api.utils.InsightsLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class ClassPageServiceImpl implements ClassPageService, InsightsConstant {

	@Autowired
	private BaseService baseService;

	@Autowired
	private SelectParamsService selectParamsService;

	@Autowired
	private CassandraService cassandraService;

	@Autowired
	private CSVBuilderService csvBuilderService;

	@Autowired
	private ExcelBuilderService excelBuilderService;

	@Resource
	private Properties filePath;

	private static final SimpleDateFormat secondsDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");

	private static final String REGEXP = "\\<[^>]*>";

	private static final String REPLACE = "";

	public ResponseParamDTO<Map<String,Object>> getClasspageCollectionUsage(String traceId, String collectionId, String data,boolean isSecure) throws Exception {

		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		
		/**
		 * Logical Authorizing API
		 */
		Map<String, String> selectValues = new HashMap<String, String>();
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		requestParamsDTO.setFields(getSelectParamsService().getClasspageCollectionUsage(requestParamsDTO.getFields(), selectValues));
		baseService.existsFilter(requestParamsDTO);
		requestParamsDTO.getFilters().setCollectionGooruOId(collectionId);

		/**
		 * Getting session
		 */
		String session = buildRowKey(traceId, requestParamsDTO);
		List<Map<String, Object>> rawData = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> aggregateData = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> intermediateData = new ArrayList<Map<String, Object>>();
		
		/**
		 * Get Collection Data
		 */
		rawData = getBaseService().getColumnValues(traceId,
				getCassandraService().getClassPageUsage(traceId, ColumnFamily.RESOURCE.getColumnFamily(), null, collectionId, new String(),
						new ArrayList<String>()));
		
		/**
		 * Thumbnail URL Logic
		 */
		Map<String,Map<Integer,String>> combineMap = new HashMap<String, Map<Integer,String>>();
		Map<Integer,String> filterMap = new HashMap<Integer, String>();
		filterMap.put(0,filePath.getProperty(ApiConstants.NFS_BUCKET));
		filterMap.put(1,ApiConstants.FOLDER);
		filterMap.put(2,ApiConstants.THUMBNAIL);
		combineMap.put(ApiConstants.THUMBNAIL, filterMap);
		rawData = getBaseService().appendInnerData(rawData, combineMap,isSecure ? ApiConstants.HTTPS : ApiConstants.HTTP);
		
		/**
		 * Will be removed once confirmed GooruOId properly inserted in Cassandra
		 */
		rawData = getBaseService().addCustomKeyInMapList(rawData,ApiConstants.GOORUOID, collectionId);
		
		/**
		 * Add lastAccessed field with the value of lastModified if it is NULL!!
		 */
		Map<String,String> fieldList = new HashMap<String, String>();
		fieldList.put(ApiConstants.LAST_ACCESSED,ApiConstants.LAST_MODIFIED);
		rawData  = getBaseService().replaceIfNull(rawData,fieldList);
		
		/**
		 * Include userCount if it is ClassPage
		 */
		if (getBaseService().notNull(requestParamsDTO.getFilters().getClassId())) {
			Map<String, Object> injuctableRecord = new HashMap<String, Object>();
			Map<String, Object> whereCondition = new HashMap<String, Object>();
			whereCondition.put(ApiConstants.CLASSPAGE_GOORU_OID, requestParamsDTO.getFilters().getClassId());
			whereCondition.put(ApiConstants.IS_GROUP_OWWNER, 0);
			whereCondition.put(ApiConstants.DELETED, 0);
			injuctableRecord.put(ApiConstants.USERCOUNT,getCassandraService().getRowCount(traceId, ColumnFamily.CLASSPAGE.getColumnFamily(), whereCondition, new ArrayList<String>()));
			rawData = getBaseService().injectRecord(rawData, injuctableRecord);
		}					

		/**
		 * Inject Aggregated Data
		 */
		Map<String, Object> injuctableRecord = new HashMap<String, Object>();
		injuctableRecord.put(ApiConstants.GOORUOID, collectionId);
		intermediateData = getBaseService().getColumnValues(traceId,
				getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session, getBaseService().appendAdditionalField(requestParamsDTO.getFields(), collectionId, new String())));
		aggregateData = getBaseService().injectRecord(intermediateData, injuctableRecord);
		injuctableRecord = new HashMap<String, Object>();
		injuctableRecord.put(ApiConstants.DELETED, 0);
		injuctableRecord.put(ApiConstants.COLLECTION_GOORU_OID, collectionId);
		OperationResult<Rows<String, String>> collectionList = cassandraService.readAll(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), injuctableRecord, new ArrayList<String>());
		
		/**
		 * Inject itemCount,ResourceCount,nonResourceCount and questionCount
		 */
		injuctableRecord = getItemCount(collectionList);
		
		aggregateData = getBaseService().injectRecord(aggregateData, injuctableRecord);
		responseParamDTO.setContent(getBaseService().properName(getBaseService().rightJoin(rawData, aggregateData, ApiConstants.GOORUOID, ApiConstants.GOORUOID), selectValues));
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> getClasspageResourceUsage(String traceId, String collectionId, String data,boolean isSecure) throws Exception {

		/**
		 * Logical Authorizing API
		 */
		Map<String, String> selectValues = new HashMap<String, String>();
		RequestParamsDTO requestParamsDTO = this.getBaseService().buildRequestParameters(data);
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		getBaseService().existsFilter(requestParamsDTO);
		boolean singleSession = false;
		requestParamsDTO.getFilters().setCollectionGooruOId(collectionId);
		requestParamsDTO.setFields(getSelectParamsService().getClasspageResourceUsage(requestParamsDTO.getFields(), selectValues));
		String session = buildRowKey(traceId, requestParamsDTO);
		 if (!requestParamsDTO.getFilters().getSession().startsWith("AS")) {
             singleSession = true;
         }
		 
			List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> rawData = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> resourceItem = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> aggregateData = new ArrayList<Map<String, Object>>();
			resourceItem = getBaseService().getRowsColumnValues(traceId, getCassandraService().getClassPageResouceUsage(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), "collection_gooru_oid", collectionId));

			/**
			 * Return if resource
			 */
			if (resourceItem.isEmpty()) {
				responseParamDTO.setContent(resourceItem);
				return responseParamDTO;
			}

			/**
			 * Extract resource ids and fetch rawData
			 */
			StringBuffer previousData = getBaseService().exportData(resourceItem, ApiConstants.RESOURCEGOORUOID);
			rawData = getBaseService()
					.getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.RESOURCE.getColumnFamily(), null, previousData.toString(), new String(), getBaseService().convertStringToCollection(requestParamsDTO.getFields())));

			/**
			 * Thumbnail URL Logic 
			 */
			Map<String, Map<Integer, String>> combineMap = new HashMap<String, Map<Integer, String>>();
			Map<Integer, String> filterMap = new HashMap<Integer, String>();
			filterMap.put(0, filePath.getProperty(ApiConstants.NFS_BUCKET));
			filterMap.put(1, ApiConstants.FOLDER);
			filterMap.put(2, ApiConstants.THUMBNAIL);
			combineMap.put(ApiConstants.THUMBNAIL, filterMap);
			rawData = getBaseService().appendInnerData(rawData, combineMap, isSecure ? ApiConstants.HTTPS : ApiConstants.HTTP);

			/**
			 * This line is added to avoid inner join failures due to
			 * unavailable gooruOId column for some cases
			 */
			rawData = getBaseService().addCustomKeyInMapList(rawData, ApiConstants.GOORUOID, null);
			/**
			 * Restructured raw data
			 */
			resourceItem = getBaseService().rightJoin(rawData, resourceItem, ApiConstants.GOORUOID, ApiConstants.RESOURCEGOORUOID);

			/**
			 * fetch aggregate data
			 */
			aggregateData = getBaseService().getColumnValues(traceId,
					getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session,
							getBaseService().appendAdditionalField(previousData.toString(), null, requestParamsDTO.getFields())));
			resultSet = getBaseService().JoinWithSingleKey(resourceItem, aggregateData, ApiConstants.GOORUOID);
			/**
			 * Get answer for the questions
			 */
			resultSet = getAnswer(traceId,resultSet, selectValues, collectionId, singleSession);

			/**
			 * get teacher response
			 */
			StringBuffer teacherUId = new StringBuffer();
			teacherUId = getBaseService().exportData(resultSet, ApiConstants.FEEDBACK_PROVIDER);
			if (!teacherUId.toString().isEmpty()) {
				String teacherUid = "";
				String[] teacherid = teacherUId.toString().split(",");
				for (String ids : teacherid) {
					if (ids != null && !ids.isEmpty() && !ids.contains("null")) {
						teacherUid = ids;
						break;
					}
				}
				selectValues.put(ApiConstants.FEEDBACK_TEACHER_NAME, ApiConstants.USER_NAME);
				selectValues.put(ApiConstants.FEEDBACK_PROVIDER, ApiConstants.GOORUUID);
				List<Map<String, Object>> teacherData = getBaseService().getColumnValues(traceId, getCassandraService().getClassPageUsage(traceId, "user", "", teacherUid, "", new ArrayList<String>()));
				teacherData = getBaseService().properName(teacherData, selectValues);
				resultSet = getBaseService().LeftJoin(resultSet, teacherData, ApiConstants.FEEDBACK_PROVIDER, ApiConstants.FEEDBACK_PROVIDER);
			}

			/**
			 * will perform custom pagination depends on the input
			 */
			if (requestParamsDTO.getPaginate() != null) {
				if (requestParamsDTO.getPaginate().getSortBy().contains(ApiConstants.TOTAL_INCORRECT_COUNT)) {

					resultSet = getBaseService().removeUnknownKeyList(resultSet, ApiConstants.TOTAL_INCORRECT_COUNT, ApiConstants.CATEGORY, ApiConstants.QUESTION, false);
				} else if (requestParamsDTO.getPaginate().getSortBy().contains(ApiConstants.TOTAL_CORRECT_COUNT)) {
					resultSet = getBaseService().removeUnknownKeyList(resultSet, ApiConstants.TOTAL_CORRECT_COUNT, ApiConstants.CATEGORY, ApiConstants.QUESTION, false);
				} else if (requestParamsDTO.getPaginate().getSortBy().contains(ApiConstants.RESPONSE)) {
					resultSet = getBaseService().removeUnknownValueList(resultSet, ApiConstants.QUESTION_TYPE, addQuestionType.OE.getQuestionType(), ApiConstants.CATEGORY,
							ApiConstants.QUESTION, false);
				}
				getBaseService().sortBy(resultSet, requestParamsDTO.getPaginate().getSortBy(), requestParamsDTO.getPaginate().getSortOrder());
			}
			responseParamDTO.setContent(resultSet);
			return responseParamDTO;
	}

	/**
	 *  To get the list of users with in the classpage
	 */
	public ResponseParamDTO<Map<String, Object>> getClasspageUsers(String traceId, String classId, String data) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, String> selectValues = new HashMap<String, String>();
		RequestParamsDTO requestParamsDTO = this.getBaseService().buildRequestParameters(data);
		requestParamsDTO.setFields(getSelectParamsService().getClasspageUser(requestParamsDTO.getFields(), selectValues));
		Map<String, Object> whereCondition = new HashMap<String, Object>();
		whereCondition.put("classpage_gooru_oid", classId);
		whereCondition.put("is_group_owner", 0);
		whereCondition.put("deleted", 0);
		responseParamDTO.setContent(getBaseService().properName(
				getBaseService().getRowsColumnValues(traceId,
						getCassandraService().readAll(traceId, ColumnFamily.CLASSPAGE.getColumnFamily(), whereCondition, getBaseService().convertStringToCollection(requestParamsDTO.getFields()))),
				selectValues));
		return responseParamDTO;
	}

	/**
	 * List the OE resources
	 */
	public ResponseParamDTO<Map<String, Object>> getClasspageResourceOEtext(String traceId,String collectionId, String data) throws Exception {

		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		boolean userId = false;
		Map<String, String> selectValues = new HashMap<String, String>();
		List<Map<String, Object>> aggregateData = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> userData = new ArrayList<Map<String, Object>>();
		requestParamsDTO.setFields(getSelectParamsService().getOEResource(requestParamsDTO.getFields(), selectValues));

		/**
		 * process validation
		 */
		getBaseService().existsFilter(requestParamsDTO);
		requestParamsDTO.getFilters().setCollectionGooruOId(collectionId);
		if (!getBaseService().notNull(requestParamsDTO.getFilters().getResourceGooruOId())) {
			throw new BadRequestException(ErrorMessages.E103 + ApiConstants.RESOURCE_GOORUOID);
		}
		if (getBaseService().notNull(requestParamsDTO.getFilters().getUserUId())) {
			userId = true;
		}

		/**
		 * For a given Single student
		 */
		if (userId) {
			String rowKey = buildRowKey(traceId, requestParamsDTO);
			aggregateData = getBaseService().getColumnValues(traceId,
					getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), rowKey,
							getBaseService().appendAdditionalField(requestParamsDTO.getFilters().getResourceGooruOId(), null, requestParamsDTO.getFields())));
			userData = getBaseService().getColumnValues(traceId,
					getCassandraService().getClassPageUsage(traceId, ColumnFamily.USER.getColumnFamily(), ApiConstants.STRING_EMPTY, requestParamsDTO.getFilters().getUserUId(), ApiConstants.STRING_EMPTY,
							new ArrayList<String>()));
			userData = getBaseService().properName(userData, selectValues);
			selectValues.put("gooruUid", ApiConstants.GOORU_UID);
			aggregateData = getBaseService().properNameEndsWith(getBaseService().RandomJoin(aggregateData, userData), selectValues);
		} else {
			/**
			 * Fetch all the students from classpage
			 */
			List<Map<String,Object>> classData = new ArrayList<Map<String,Object>>();
			Map<String, Object> whereCondition = new HashMap<String, Object>();
			whereCondition.put(ApiConstants.CLASSPAGE_GOORU_OID, requestParamsDTO.getFilters().getClassId());
			whereCondition.put(ApiConstants.IS_GROUP_OWWNER, 0);
			whereCondition.put(ApiConstants.DELETED, 0);
			classData = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.CLASSPAGE.getColumnFamily(), whereCondition, new ArrayList<String>()));
			StringBuffer userUId = getBaseService().exportData(classData, ApiConstants.GOORU_UID);
			if (getBaseService().notNull(userUId.toString())) {
				for (String gooruUId : userUId.toString().split(ApiConstants.COMMA)) {
					requestParamsDTO.getFilters().setUserUId(gooruUId);
					String rowKey = buildRowKey(traceId, requestParamsDTO);
					Collection<String> columnName = getBaseService().appendAdditionalField(requestParamsDTO.getFilters().getResourceGooruOId(), null, requestParamsDTO.getFields());
					columnName.add(ApiConstants.GOORU_UID);
					aggregateData.add(getBaseService().getColumnValue(traceId,
							getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), rowKey,
									columnName)));
				}
			}
			/**
			 * Get student detail
			 */
			userData = getBaseService().getRowsColumnValues(traceId,
					getCassandraService().readAll(traceId, ColumnFamily.USER.getColumnFamily(), getBaseService().convertStringToCollection(userUId.toString()),
							getBaseService().convertStringToCollection(requestParamsDTO.getFields())));
			userData = getBaseService().properName(userData, selectValues);
			selectValues.put("gooruUid", ApiConstants.GOORU_UID);
			classData = getBaseService().properNameEndsWith(classData,selectValues);
			aggregateData = getBaseService().properNameEndsWith(aggregateData,selectValues);
			aggregateData = getBaseService().LeftJoin(classData, aggregateData, ApiConstants.GOORUUID, ApiConstants.GOORUUID);
			aggregateData = getBaseService().LeftJoin(userData, aggregateData, ApiConstants.GOORUUID, ApiConstants.GOORUUID);
		}
		aggregateData = getBaseService().sortBy(aggregateData, ApiConstants.USERNAME, ApiConstants.ASC);
		responseParamDTO.setContent(aggregateData);
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> getUserSessions(String traceId, String data, String collectionId) throws Exception {
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		getBaseService().existsFilter(requestParamsDTO);
		requestParamsDTO.getFilters().setCollectionGooruOId(collectionId);
		String filterValues = "";
		if (requestParamsDTO.getFilters().getClassId() != null && !requestParamsDTO.getFilters().getClassId().isEmpty()) {
			if (getBaseService().notNull(requestParamsDTO.getFilters().getPathwayId())) {
				filterValues = requestParamsDTO.getFilters().getClassId() + ApiConstants.TILDA + requestParamsDTO.getFilters().getPathwayId() + ApiConstants.TILDA + collectionId + ApiConstants.TILDA
						+ requestParamsDTO.getFilters().getUserUId();
			} else {
				filterValues = requestParamsDTO.getFilters().getClassId() + ApiConstants.TILDA + collectionId + ApiConstants.TILDA + requestParamsDTO.getFilters().getUserUId();
			}
		} else {
			filterValues = collectionId + ApiConstants.TILDA + requestParamsDTO.getFilters().getUserUId();
		}
		/**
		 * Fetch the list of session ids with timestamp
		 */
		OperationResult<ColumnList<String>> columnResult = getCassandraService().read(traceId, ColumnFamily.MICRO_AGGREGATION.getColumnFamily(), filterValues);
		if (!columnResult.getResult().isEmpty()) {
			List<Map<String, Object>> sessionList = new ArrayList<Map<String, Object>>();
			for (Column<String> column : columnResult.getResult()) {
				Map<String, Object> sessionTimeStamp = new HashMap<String, Object>();
				sessionTimeStamp.put("sessionId", column.getName());
				try {
					Date date = secondsDateFormatter.parse(column.getStringValue());
					sessionTimeStamp.put("timeStamp", Math.abs(date.getTime()));
				} catch (ParseException e) {
					InsightsLogger.error(traceId, e);
				}
				sessionList.add(sessionTimeStamp);
			}
			if (requestParamsDTO.getPaginate() != null) {
				Map<String, Object> injuctableRecord = new HashMap<String, Object>();
				injuctableRecord.put("counter", 1);
				injuctableRecord.put("key", "frequency");
				sessionList = getBaseService().sortBy(sessionList, requestParamsDTO.getPaginate().getSortBy(), requestParamsDTO.getPaginate().getSortOrder());
				sessionList = getBaseService().injectCounterRecord(sessionList, injuctableRecord);
			}
			responseParamDTO.setContent(sessionList);
			return responseParamDTO;
		}
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> getClasspageUserUsage(String traceId, String collectionId, String data,boolean isSecure) throws Exception {

		/**
		 * Logical Authorizing API
		 */
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, String> selectValues = new HashMap<String, String>();
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		getBaseService().existsFilter(requestParamsDTO);
		requestParamsDTO.getFilters().setCollectionGooruOId(collectionId);
		requestParamsDTO.setFields(getSelectParamsService().getClasspageResourceUsage(requestParamsDTO.getFields(), selectValues));
		try {
			boolean singleSession = false;
			if (!requestParamsDTO.getFilters().getSession().startsWith("AS")) {
				singleSession = true;
			}
			
			List<Map<String, Object>> rawData = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> resourceData = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> itemData = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> aggregateData = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>();
			StringBuffer previousData = new StringBuffer();

			/**
			 * Get list of resources and it's meta-data
			 */
			itemData = getBaseService().getRowsColumnValues(traceId,
					getCassandraService().getClassPageResouceUsage(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTION_GOORU_OID, collectionId));

			if (itemData.isEmpty()) {
				responseParamDTO.setContent(itemData);
				return responseParamDTO;
			}
			previousData = getBaseService().exportData(itemData, ApiConstants.RESOURCEGOORUOID);
			rawData = getBaseService()
					.getRowsColumnValues(traceId,getCassandraService().readAll(traceId, ColumnFamily.RESOURCE.getColumnFamily(), null, previousData.toString(), new String(), new ArrayList<String>()));

			Map<String, Map<Integer, String>> combineMap = new HashMap<String, Map<Integer, String>>();
			Map<Integer, String> filterMap = new HashMap<Integer, String>();
			filterMap.put(0, filePath.getProperty(ApiConstants.NFS_BUCKET));
			filterMap.put(1, ApiConstants.FOLDER);
			filterMap.put(2, ApiConstants.THUMBNAIL);
			combineMap.put(ApiConstants.THUMBNAIL, filterMap);
			rawData = getBaseService().appendInnerData(rawData, combineMap, isSecure ? ApiConstants.HTTPS : ApiConstants.HTTP);

			/**
			 * This line is added to avoid inner join failures due to
			 * unavailable gooruOId column for some cases
			 */
			rawData = getBaseService().addCustomKeyInMapList(rawData, "gooruOId", null);

			resourceData = getBaseService().LeftJoin(itemData, rawData, "resource_gooru_oid", ApiConstants.GOORUOID);
			resourceData = getBaseService().properName(resourceData, selectValues);

			String session = requestParamsDTO.getFilters().getSession();
			/**
			 * Get list of users in the class
			 */
			Map<String, Object> whereCondition = new HashMap<String, Object>();
			whereCondition.put("classpage_gooru_oid", requestParamsDTO.getFilters().getClassId());
			whereCondition.put("is_group_owner", 0);
			whereCondition.put("deleted", 0);
			List<Map<String, Object>> classData = new ArrayList<Map<String, Object>>();
			classData = getBaseService().getRowsColumnValues(traceId,getCassandraService().readAll(traceId, ColumnFamily.CLASSPAGE.getColumnFamily(), whereCondition, new ArrayList<String>()));

			StringBuffer userUId = getBaseService().exportData(classData, ApiConstants.GOORU_UID);
			if (getBaseService().notNull(userUId.toString())) {
				if (session.startsWith("FS")) {
					for (String userId : userUId.toString().split(",")) {
						requestParamsDTO.getFilters().setUserUId(userId);
						session = buildRowKey(traceId, requestParamsDTO);
						Map<String, Object> temp = new HashMap<String, Object>();
						temp = getBaseService().getColumnValues(traceId, getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session, new ArrayList<String>()), temp);
						aggregateData.add(temp);
					}
				} else {
					aggregateData = getBaseService().getRowsColumnValues(traceId,
							getCassandraService().readAll(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session, "~", userUId.toString(), new ArrayList<String>()));
				}
				aggregateData = getBaseService().getSingleKey(aggregateData, "gooru_oid", "~gooru_oid");

				/**
				 * Get user related info
				 */
				rawData = getBaseService().getRowsColumnValues(traceId,getCassandraService().readAll(traceId, "user", "", userUId.toString(), "", new ArrayList<String>()));
				classData = getBaseService().LeftJoin(classData, rawData, "gooru_uid", "gooru_uid");
				selectValues.put("gooruUId", "gooru_uid");
				classData = getBaseService().properName(classData, selectValues);
				classData = getBaseService().combineTwoList(resourceData, classData, "resourceGooruOId", "gooruUId");
				resultData = getBaseService().LeftJoin(aggregateData, rawData, "gooru_uid", "gooru_uid");

			}
			/**
			 * Get question resource answers
			 */
			Map<String, String> surName = new HashMap<String, String>();
			Collection<String> additionParameter = new ArrayList<String>();
			additionParameter.add("providedAnswer");
			surName.put("~A", "A");
			surName.put("~B", "B");
			surName.put("~C", "C");
			surName.put("~D", "D");
			surName.put("~E", "E");
			resultData = getBaseService().InnerJoinContainsKey(resultData, itemData, "gooru_oid", "resource_gooru_oid");
			aggregateData = getBaseService().buildJSON(traceId, resultData, additionParameter, surName, singleSession);
			selectValues.put("options", "providedAnswer");
			surName = new HashMap<String, String>();
			surName.put("correct", "is_correct");
			surName.put("resourceGooruOId", "question_gooru_oid");
			surName.put("sequence", "sequence");
			surName.put("typeName", "type_name");
			surName.put("answer", "answer_text");
			surName.put("answerId", "answer_id");
			surName.put("metaData", "metaData");
			rawData = getBaseService().properName(
					getBaseService().getData(
							getBaseService().getRowsColumnValues(traceId,
									getCassandraService().readAll(traceId, ColumnFamily.ASSESSMENT_ANSWER.getColumnFamily(), "collection_gooru_oid", collectionId, new ArrayList<String>())),
							"question_gooru_oid"), surName);
			selectValues.put("resourceGooruOId", "gooru_oid");
			aggregateData = getBaseService().properName(aggregateData, selectValues);
			aggregateData = getBaseService().leftJoinwithTwoKey(classData, aggregateData, "resourceGooruOId", "gooruUId");
			aggregateData = getBaseService().sortBy(aggregateData, "userName", "ASC");
			aggregateData = getBaseService().getUserData(aggregateData, "resourceGooruOId", null, "userName", null, null);
			aggregateData = getBaseService().safeJoin(aggregateData, rawData, "resourceGooruOId", "resourceGooruOId");
			aggregateData = getBaseService().LeftJoin(resourceData, aggregateData, "resourceGooruOId", "resourceGooruOId");

			if (requestParamsDTO.getPaginate() != null) {
				getBaseService().sortBy(aggregateData, requestParamsDTO.getPaginate().getSortBy(), requestParamsDTO.getPaginate().getSortOrder());
			}
			responseParamDTO.setContent(aggregateData);
			return responseParamDTO;

		} catch (ParseException e) {
			InsightsLogger.error(traceId, e);
		}
		return new ResponseParamDTO<Map<String, Object>>();
	}

	public ResponseParamDTO<Map<String, Object>> getResourceInfo(String traceId, String resouceId, String data) {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		ColumnList<String> resourceInfo = getCassandraService().read(traceId, ColumnFamily.LIVE_DASHBOARD.getColumnFamily(), "all~" + resouceId).getResult();
		Map<String, Object> resourceInfoMap = new LinkedHashMap<String, Object>();
		List<Map<String, Object>> resourceInfoList = new ArrayList<Map<String, Object>>();
		for (int i = 0; i < resourceInfo.size(); i++) {
			resourceInfoMap.put(this.getApiDisplayName(resourceInfo.getColumnByIndex(i).getName()), resourceInfo.getColumnByIndex(i).getLongValue());
		}
		resourceInfoList.add(resourceInfoMap);
		responseParamDTO.setContent(resourceInfoList);
		return responseParamDTO;

	}

	private String getApiDisplayName(String columnName) {

		String columnVal = "";
		columnName = columnName.replaceAll("_", "");
		columnName = columnName.replaceAll("-", "");
		columnName = columnName.replaceAll("\\.", "");

		for (String splittedString : columnName.split("~")) {
			columnVal += splittedString.substring(0, 1).toUpperCase() + splittedString.substring(1);
		}

		columnVal = columnVal.substring(0, 1).toLowerCase() + columnVal.substring(1);

		return columnVal;
	}

	public ResponseParamDTO<Map<String, Object>> getClasspageGrade(String traceId, String classId, String data,boolean isSecure) throws Exception {

		/**
		 * Logical Authorizing API
		 */
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, String> selectValues = new HashMap<String, String>();
		boolean isAggregateData = false;
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		requestParamsDTO.setFields(getSelectParamsService().getClasspageResourceUsage(requestParamsDTO.getFields(), selectValues));
		requestParamsDTO.getFilters().setClassId(classId);

		List<Map<String, Object>> rawData = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> resourceData = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> aggregateData = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>();
		StringBuffer previousData = new StringBuffer();

		/**
		 * Get list of collections from class page or pathway
		 */
		Map<String, Object> whereCondition = new HashMap<String, Object>();
		if (getBaseService().notNull(requestParamsDTO.getFilters().getPathwayId())) {
			whereCondition.put("collection_gooru_oid", requestParamsDTO.getFilters().getPathwayId());
		} else {
			whereCondition.put("collection_gooru_oid", classId);
		}
		if (getBaseService().notNull(requestParamsDTO.getFilters().getCollectionGooruOId())) {
			whereCondition.put("resource_gooru_oid", requestParamsDTO.getFilters().getCollectionGooruOId());
		}
		whereCondition.put("deleted", 0);

		resourceData = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), whereCondition, new ArrayList<String>()));

		StringBuffer itemData = getBaseService().exportData(resourceData, ApiConstants.COLLECTIONITEMID);

		if (resourceData.isEmpty()) {
			responseParamDTO.setContent(resourceData);
			return responseParamDTO;
		}

		/**
		 * Get only collection from the classpage,in order to avoid pathway in
		 * the list
		 */
		previousData = getBaseService().exportData(resourceData, ApiConstants.RESOURCEGOORUOID);
		List<String> collection = new ArrayList<String>();
		List<String> collectionColumn = new ArrayList<String>();
		collectionColumn.add("collection_type");
		collectionColumn.add("gooru_oid");
		List<Map<String, Object>> collectionData = getBaseService().getRowsColumnValues(traceId,
				getCassandraService().readAll(traceId, ColumnFamily.COLLECTION.getColumnFamily(), null, previousData.toString(), new String(), collectionColumn));
		for (Map<String, Object> map : collectionData) {
			if (map.containsKey("collection_type") && map.containsKey("gooru_oid")
					&& (map.get("collection_type").toString().equalsIgnoreCase("collection") || map.get("collection_type").toString().equalsIgnoreCase("assessment"))) {
				collection.add(map.get("gooru_oid").toString());
			}
		}

		List<Map<String, Object>> tempList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> collectionItem : resourceData) {
			Map<String, Object> map = new HashMap<String, Object>();
			if (collectionItem.containsKey("resource_gooru_oid") && collection.contains(collectionItem.get("resource_gooru_oid"))) {
				map.putAll(collectionItem);
				tempList.add(map);
			}
		}
		resourceData = tempList;
		tempList = new ArrayList<Map<String, Object>>();
		if (resourceData.isEmpty()) {
			return new ResponseParamDTO<Map<String, Object>>();
		}
		/**
		 * Get resource data
		 */
		rawData = getBaseService().getRowsColumnValues(traceId,
				getCassandraService().readAll(traceId, ColumnFamily.RESOURCE.getColumnFamily(), null, getBaseService().convertListToString(collection), new String(), new ArrayList<String>()));
		Map<String, Map<Integer, String>> combineMap = new HashMap<String, Map<Integer, String>>();
		Map<Integer, String> filterMap = new HashMap<Integer, String>();
		filterMap.put(0, filePath.getProperty(ApiConstants.NFS_BUCKET));
		filterMap.put(1, ApiConstants.FOLDER);
		filterMap.put(2, ApiConstants.THUMBNAIL);
		combineMap.put(ApiConstants.THUMBNAIL, filterMap);
		rawData = getBaseService().appendInnerData(rawData, combineMap, isSecure ? ApiConstants.HTTPS : ApiConstants.HTTP);
		/**
		 * This line is added to avoid inner join failures due to unavailable
		 * gooruOId column for some cases
		 */
		rawData = getBaseService().addCustomKeyInMapList(rawData, "gooruOId", null);

		resourceData = getBaseService().LeftJoin(resourceData, rawData, "resource_gooru_oid", "gooruOId");
		resourceData = getBaseService().properName(resourceData, selectValues);

		/**
		 * Get aggregate data
		 */
		StringBuffer userUId = new StringBuffer();
		List<Map<String, Object>> classData = new ArrayList<Map<String, Object>>();
		String session = buildRowKey(traceId, requestParamsDTO);
		if (getBaseService().notNull(requestParamsDTO.getFilters().getUserUId())) {
			userUId = new StringBuffer(requestParamsDTO.getFilters().getUserUId());
		} else {
			whereCondition = new HashMap<String, Object>();
			whereCondition.put("classpage_gooru_oid", classId);
			whereCondition.put("is_group_owner", 0);
			whereCondition.put("deleted", 0);
			if (getBaseService().notNull(requestParamsDTO.getFilters().getUserUId())) {
				whereCondition.put("gooru_uid", requestParamsDTO.getFilters().getUserUId());
			}
			classData = getBaseService().getRowsColumnValues(traceId,getCassandraService().readAll(traceId, ColumnFamily.CLASSPAGE.getColumnFamily(), whereCondition, new ArrayList<String>()));

			userUId = getBaseService().exportData(classData, ApiConstants.GOORU_UID);
		}
		Collection<String> extractId = new ArrayList<String>();
		if (getBaseService().notNull(userUId.toString())) {
			extractId = getBaseService().appendAdditionalField(previousData.toString(), new String(), "~views,~avg_time_spent,~gooru_oid,~score,~time_spent,~grade_in_percentage");
			extractId.add("gooru_uid");
			aggregateData = getBaseService().getRowsColumnValues(traceId,
					getCassandraService().readAll(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session + "~", previousData.toString(),
							getBaseService().appendAdditionalField(userUId.toString(), "~", new String()), extractId));
			aggregateData = getBaseService().getSingleKey(aggregateData, "gooru_oid", "~gooru_oid");
			if (aggregateData != null && !aggregateData.isEmpty()) {
				isAggregateData = true;
			}
			
			/**
			 * Get user details
			 */
			rawData = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId,"user", "", userUId.toString(), "", new ArrayList<String>()));
			Map<String, Object> injuctableRecord = new HashMap<String, Object>();
			injuctableRecord.put("profile_url", filePath.getProperty("insights.profile.url.path"));
			rawData = getBaseService().formatRecord(rawData, injuctableRecord, "gooruUid", "id");
			classData = getBaseService().LeftJoin(rawData, classData, "gooruUid", "gooru_uid");
			classData = getBaseService().properName(classData, selectValues);
			classData = getBaseService().combineTwoList(resourceData, classData, "resourceGooruOId", "gooruUId");
			resultData = getBaseService().LeftJoin(rawData, aggregateData, "gooruUid", "gooru_uid");
		}

		/**
		 * fetching Collection minimum score,studing and status info
		 */
		if (!classData.isEmpty()) {
			userUId = getBaseService().exportData(classData, ApiConstants.GOORU_UID);
			List<Map<String, Object>> processData = getBaseService().getRowsColumnValues(traceId,
					getCassandraService().readAll(traceId, ColumnFamily.USER_COLLECTION_ITEM_ASSOC.getColumnFamily(), userUId.toString(), "~", itemData.toString(), new ArrayList<String>()));

			Map<String, String> assocFields = new HashMap<String, String>();
			assocFields.put("userTime", "time_studying");
			assocFields.put("collectionStatus", "collection_status");
			assocFields.put("resourceGooruOId", "gooru_oid");
			assocFields.put("gooruUId", "user_uid");
			assocFields.put("minimumScore", "minimum_score");

			processData = getBaseService().properName(processData, assocFields);
			selectValues.put("resourceGooruOId", "gooru_oid");
			resultData = getBaseService().properName(resultData, selectValues);

			resultData = getBaseService().leftJoinwithTwoKey(classData, resultData, "resourceGooruOId", "gooruUId");

			aggregateData = getBaseService().leftJoinwithTwoKey(resultData, processData, "resourceGooruOId", "gooruUId");

		}

		
		/**
		 * Custom pagination,as per product team suggestion default student count is 3
		 */
		if (requestParamsDTO.getPaginate() != null) {
			aggregateData = getBaseService().getUserData(aggregateData, "resourceGooruOId", null, requestParamsDTO.getPaginate().getSortBy(), requestParamsDTO.getPaginate().getSortOrder(),
					baseService.notNull(requestParamsDTO.getPaginate().getTotalRecords()) ? requestParamsDTO.getPaginate().getTotalRecords() : 3);
		} else {
			aggregateData = getBaseService().getUserData(aggregateData, "resourceGooruOId", null, null, null, null);

		}
		aggregateData = getBaseService().safeJoin(aggregateData, rawData, "resourceGooruOId", "resourceGooruOId");
		aggregateData = getBaseService().LeftJoin(resourceData, aggregateData, "resourceGooruOId", "resourceGooruOId");

		if (requestParamsDTO.getPaginate() != null) {
			getBaseService().sortBy(aggregateData, requestParamsDTO.getPaginate().getSortBy(), requestParamsDTO.getPaginate().getSortOrder());
		}
		Map<String, Object> isData = new HashMap<String, Object>();
		isData.put("aggregateData", String.valueOf(isAggregateData));

		responseParamDTO.setContent(aggregateData);
		responseParamDTO.setMessage(isData);
		return responseParamDTO;
	}

	public ResponseParamDTO<Map<String, Object>> getExportGradeBook(String traceId, String classId, String data, String reportType, String timeZone) throws Exception {

		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> classGrade = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classRawdata = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classGradeInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> collectionData = new ArrayList<Map<String, Object>>();
		Map<String, Object> classData = new HashMap<String, Object>();
		/**
		 * pre validation
		 */
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		getBaseService().existsFilter(requestParamsDTO);
		requestParamsDTO.getFilters().setClassId(classId);
		String session = requestParamsDTO.getFilters().getSession();

		/**
		 * fetch resource data
		 */
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add("title");
		resourceColumns.add("createdOn");
		classData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), classId, resourceColumns));

		/**
		 * fetch classpage data
		 */
		Map<String, Object> whereCondition = new HashMap<String, Object>();
		whereCondition.put("classpage_gooru_oid", classId);
		whereCondition.put("is_group_owner", 0);
		whereCondition.put("deleted", 0);
		classRawdata = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.CLASSPAGE.getColumnFamily(), whereCondition, new ArrayList<String>()));

		/**
		 * fetch list of collections
		 */
		if (getBaseService().notNull(requestParamsDTO.getFilters().getCollectionGooruOId())) {
			whereCondition = new HashMap<String, Object>();
			whereCondition.put("collection_gooru_oid", classId);
			whereCondition.put("resource_gooru_oid", requestParamsDTO.getFilters().getCollectionGooruOId());
			collectionData = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), whereCondition, new ArrayList<String>()));
		} else {
			collectionData = getBaseService().getRowsColumnValues(traceId, getCassandraService().getClassPageResouceUsage(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), "collection_gooru_oid", classId));
		}
		if (collectionData.isEmpty()) {
			responseParamDTO.setContent(collectionData);
			return responseParamDTO;
		} else {
			collectionData = getBaseService().sortBy(collectionData, "item_sequence", "DESC");
		}

		Map<String, Object> classMapInfo = new LinkedHashMap<String, Object>();
		classMapInfo.put("Class Title", classData.get("title") != null ? String.valueOf(classData.get("title")).replaceAll(REGEXP, REPLACE) : "-");
		try {
			SimpleDateFormat formatter2 = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
			Date utcDate = new Date(Long.valueOf(classData.get("createdOn").toString()));
			Date clientDate = getBaseService().convertTimeZone(utcDate, "UTC", timeZone);
			String date = formatter2.format(clientDate);
			classMapInfo.put("Date Created", date);
		} catch (Exception e) {
			InsightsLogger.error(traceId, e);
		}
		classInfo.add(classMapInfo);
		classGrade.add(classMapInfo);

		/**
		 * fetch aggregate data for all the collection
		 */
		for (Map<String, Object> userClass : classRawdata) {
			Map<String, Object> collectionGrade = new LinkedHashMap<String, Object>();
			collectionGrade.put("Student", userClass.get("username"));
			for (Map<String, Object> singleCollection : collectionData) {
				Map<String, Object> collectionRawData = new HashMap<String, Object>();
				collectionRawData = getBaseService().getColumnValue(traceId,
						getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), "GLP~" + singleCollection.get("resource_gooru_oid"), resourceColumns));

				Map<String, Object> aggData = new HashMap<String, Object>();
				requestParamsDTO.getFilters().setCollectionGooruOId(singleCollection.get("resource_gooru_oid").toString());
				requestParamsDTO.getFilters().setUserUId(userClass.get("gooru_uid").toString());
				session = buildRowKey(traceId, requestParamsDTO);
					aggData = getBaseService().getColumnValue(traceId,
							getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(),session));

				String title = collectionRawData.get("title") != null ? collectionRawData.get("title").toString().replaceAll(REGEXP, REPLACE) : "-";

				collectionGrade.put(
						title + " - Score",
						aggData.get(singleCollection.get("resource_gooru_oid") + "~grade_in_percentage") == null ? "-" : aggData.get(singleCollection.get("resource_gooru_oid")
								+ "~grade_in_percentage"));
				collectionGrade.put(
						title + " - Time Spent",
						aggData.get(singleCollection.get("resource_gooru_oid") + "~time_spent") == null ? "-" : this.getHourlyBasedTimespent(Long.parseLong(aggData.get(
								singleCollection.get("resource_gooru_oid") + "~time_spent").toString())));
				collectionGrade.put(
						title + " - # Correct / Total Qns",
						aggData.get(singleCollection.get("resource_gooru_oid") + "~question_count") == null ? "-" : aggData.get(singleCollection.get("resource_gooru_oid") + "~score") + "/"
								+ aggData.get(singleCollection.get("resource_gooru_oid") + "~question_count"));

			}
			classGradeInfo.add(collectionGrade);
			classGrade.add(collectionGrade);
		}

		if (!reportType.equalsIgnoreCase("json")) {
			Map<String, Object> files = new HashMap<String, Object>();
			String fileName = null;
			try {
				fileName = excelBuilderService.exportXlsReport(traceId, classInfo, "Student-Grade" + "." + reportType, true);
				fileName = excelBuilderService.exportXlsReport(traceId, classGradeInfo, "Student-Grade" + "." + reportType, false);
			} catch (ParseException e) {
				InsightsLogger.error(traceId, e);
			} catch (IOException e) {
				InsightsLogger.error(traceId, e);
			}
			files.put("file", fileName);
			classGrade = new ArrayList<Map<String, Object>>();
			classGrade.add(files);
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		} else {
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		}
	}

	public ResponseParamDTO<Map<String, Object>> getClasspageCollectionOEResources(String traceId, String classId, String data, String reportType, String timeZone) throws Exception {

		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> classGrade = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classRawdata = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classOEResponseInfo = new ArrayList<Map<String, Object>>();
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);

		requestParamsDTO.getFilters().setClassId(classId);
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add("title");
		resourceColumns.add("createdOn");

		/**
		 * fetch resource,classpage and collection item data
		 */
		Map<String, Object> classData = new HashMap<String, Object>();
		classData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), classId, resourceColumns));
		Map<String, Object> whereCondition = new HashMap<String, Object>();
		whereCondition.put("classpage_gooru_oid", classId);
		whereCondition.put("is_group_owner", 0);
		whereCondition.put("deleted", 0);
		classRawdata = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.CLASSPAGE.getColumnFamily(), whereCondition, new ArrayList<String>()));

		whereCondition = new HashMap<String, Object>();
		whereCondition.put("collection_gooru_oid", classId);
		whereCondition.put("deleted", 0);
		List<Map<String, Object>> collectionData = getBaseService().getRowsColumnValues(traceId,
				getCassandraService().readAll(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), whereCondition, new ArrayList<String>()));

		if (collectionData.isEmpty()) {
			responseParamDTO.setContent(collectionData);
			return responseParamDTO;
		} else {
			collectionData = getBaseService().sortBy(collectionData, "item_sequence", "ASC");
		}
		Map<String, Object> classMapInfo = new LinkedHashMap<String, Object>();
		classMapInfo.put("Class Title", classData.get("title") != null ? String.valueOf(classData.get("title")).replaceAll(REGEXP, REPLACE) : "-");
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
			Date utcDate = new Date(Long.valueOf(classData.get("createdOn").toString()));
			Date clientDate = getBaseService().convertTimeZone(utcDate, "UTC", timeZone);
			String date = formatter.format(clientDate);
			classMapInfo.put("Date Created", date);
		} catch (Exception e) {
			InsightsLogger.error(traceId, e);
		}
		classInfo.add(classMapInfo);
		classGrade.add(classMapInfo);

		for (Map<String, Object> userClass : classRawdata) {
			Map<String, Object> collectionGrade = new LinkedHashMap<String, Object>();
			collectionGrade.put("Student", userClass.get("username"));

			/**
			 * fetch collection data one by one with respect to single user
			 */
			for (int i = 0; i < collectionData.size(); i++) {
				Map<String, Object> singleCollection = collectionData.get(i);
				Map<String, Object> collectionRawData = new HashMap<String, Object>();
				collectionRawData = getBaseService().getColumnValue(traceId,
						getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), "" + singleCollection.get("resource_gooru_oid"), resourceColumns));
				List<Map<String, Object>> resourceData = new ArrayList<Map<String, Object>>();
				Map<String, Object> Condition = new HashMap<String, Object>();
				Condition.put("collection_gooru_oid", singleCollection.get("resource_gooru_oid"));
				Condition.put("question_type", "OE");
				Condition.put("deleted", 0);
				resourceData = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), Condition, new ArrayList<String>()));
				Map<String, Object> aggData = new HashMap<String, Object>();
				/**
				 * Fetch session level aggregation data
				 */
				requestParamsDTO.getFilters().setClassId(classId);
				requestParamsDTO.getFilters().setCollectionGooruOId(singleCollection.get("resource_gooru_oid").toString());
				requestParamsDTO.getFilters().setUserUId(userClass.get("gooru_uid").toString());
				requestParamsDTO.getFilters().setCollectionGooruOId(singleCollection.get("resource_gooru_oid").toString());
				String session = buildRowKey(traceId, requestParamsDTO);
				aggData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session));

				if (!resourceData.isEmpty()) {
					resourceData = getBaseService().sortBy(resourceData, "item_sequence", "ASC");
				}
				for (int j = 0; j < resourceData.size(); j++) {
					Map<String, Object> singleResource = resourceData.get(j);
					Map<String, Object> questionRawData = new HashMap<String, Object>();
					questionRawData = getBaseService().getColumnValue(traceId,
							getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), "" + singleResource.get("resource_gooru_oid"), resourceColumns));
					StringBuffer mapKey = new StringBuffer();
					mapKey.append("[");
					mapKey.append(collectionRawData.get("title") != null ? String.valueOf(collectionRawData.get("title")).replaceAll(REGEXP, REPLACE) : "-");
					mapKey.append("]");
					mapKey.append(questionRawData.get("title") != null ? String.valueOf(questionRawData.get("title")).replaceAll(REGEXP, REPLACE) : "-");
					String text = "-";
					try {
						text = (aggData.get(singleResource.get("resource_gooru_oid") + "~choice") == null || aggData.get(singleResource.get("resource_gooru_oid") + "~choice").toString().isEmpty()) ? "-"
								: URLDecoder.decode(String.valueOf(aggData.get(singleResource.get("resource_gooru_oid") + "~choice")), "UTF-8");
					} catch (Exception e) {
						text = (aggData.get(singleResource.get("resource_gooru_oid") + "~choice") == null || aggData.get(singleResource.get("resource_gooru_oid") + "~choice").toString().isEmpty()) ? "-"
								: String.valueOf(aggData.get(singleResource.get("resource_gooru_oid") + "~choice"));
					}
					collectionGrade.put(mapKey.toString(), text == null ? "-" : text);
				}
			}
			classOEResponseInfo.add(collectionGrade);
			getBaseService().sortBy(classOEResponseInfo, "Student", "ASC");
			classGrade.add(collectionGrade);
		}

		if (!reportType.equalsIgnoreCase("json")) {
			Map<String, Object> files = new HashMap<String, Object>();
			String fileName = null;
			try {
				fileName = excelBuilderService.exportXlsReport(traceId, classInfo, "Student-Responses" + "." + reportType, true);
				fileName = excelBuilderService.exportXlsReport(traceId, classOEResponseInfo, "Student-Responses" + "." + reportType, false);
			} catch (ParseException e) {
				InsightsLogger.error(traceId, e);
			} catch (IOException e) {
				InsightsLogger.error(traceId, e);
			}
			files.put("file", fileName);
			classGrade = new ArrayList<Map<String, Object>>();
			classGrade.add(files);
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		} else {
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		}
	}

	public ResponseParamDTO<Map<String, Object>> getExportSummary(String traceId, String classId, String data, String reportType, String timeZone) throws Exception {

		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> classGrade = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classRawdata = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> collectionInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> resourcesInfo = new ArrayList<Map<String, Object>>();

		/**
		 * pre validation
		 */
		RequestParamsDTO requestParamsDTO = this.getBaseService().buildRequestParameters(data);
		getBaseService().existsFilter(requestParamsDTO);
		requestParamsDTO.getFilters().setClassId(classId);
		if (!getBaseService().notNull(requestParamsDTO.getFilters().getCollectionGooruOId())) {
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		}
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add("title");
		resourceColumns.add("createdOn");
		resourceColumns.add("resourceType");
		Map<String, String> reactionMap = new HashMap<String, String>();
		reactionMap.put("1", "I need help");
		reactionMap.put("2", "I dont understand");
		reactionMap.put("3", "meh");
		reactionMap.put("4", "I understand");
		reactionMap.put("5", "I can explain");
		reactionMap.put("10", "-");
		Map<String, Object> classData = new HashMap<String, Object>();
		classData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), classId, resourceColumns));
		/**
		 * Get class data
		 */
		Map<String, Object> whereCondition = new HashMap<String, Object>();
		whereCondition.put("classpage_gooru_oid", classId);
		whereCondition.put("is_group_owner", 0);
		whereCondition.put("deleted", 0);
		classRawdata = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.CLASSPAGE.getColumnFamily(), whereCondition, new ArrayList<String>()));

		whereCondition = new HashMap<String, Object>();
		whereCondition.put("collection_gooru_oid", classId);
		whereCondition.put("deleted", 0);
		if (getBaseService().notNull(requestParamsDTO.getFilters().getUserUId())) {
			whereCondition.put("gooru_uid", requestParamsDTO.getFilters().getUserUId());
		}

		List<Map<String, Object>> collectionData = getBaseService().getRowsColumnValues(traceId,
				getCassandraService().readAll(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), whereCondition, new ArrayList<String>()));

		if (collectionData.isEmpty()) {
			responseParamDTO.setContent(collectionData);
			return responseParamDTO;
		} else {
			collectionData = getBaseService().sortBy(collectionData, "item_sequence", "ASC");
		}
		Map<String, Object> classMapInfo = new LinkedHashMap<String, Object>();
		classMapInfo.put("Class Title", classData.get("title") != null ? String.valueOf(classData.get("title")).replaceAll(REGEXP, REPLACE) : "-");
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
			Date utcDate = new Date(Long.valueOf(classData.get("createdOn").toString()));
			Date clientDate = getBaseService().convertTimeZone(utcDate, "UTC", timeZone);
			String date = formatter.format(clientDate);
			classMapInfo.put("Class Date Created", date);
		} catch (Exception e) {
			InsightsLogger.error(traceId, e);
		}
		classInfo.add(classMapInfo);
		classGrade.add(classMapInfo);

		Map<String, Object> collectionRawData = new LinkedHashMap<String, Object>();

		Map<String, Object> collectionMapInfo = new LinkedHashMap<String, Object>();
		collectionRawData = getBaseService()
				.getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), requestParamsDTO.getFilters().getCollectionGooruOId(), resourceColumns));
		collectionMapInfo.put("Collection Title", collectionRawData.get("title") != null ? String.valueOf(collectionRawData.get("title")).replaceAll(REGEXP, REPLACE) : "-");

		try {
			SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
			Date utcDate = new Date(Long.valueOf(collectionRawData.get("createdOn").toString()));
			Date clientDate = getBaseService().convertTimeZone(utcDate, "UTC", timeZone);
			String date = formatter.format(clientDate);
			collectionMapInfo.put("Collection Date Created", date);
		} catch (Exception e) {
			InsightsLogger.error(traceId, e);
		}
		  collectionInfo.add(collectionMapInfo);

		classGrade.add(collectionMapInfo);

		boolean userFilter = false;
		if (getBaseService().notNull(requestParamsDTO.getFilters().getUserUId())) {
			userFilter = true;
		}
		/**
		 * fetch data from every user
		 */
		List<String> classStudents = new ArrayList<String>();
		for (int i = 0; i < classRawdata.size(); i++) {
			Map<String, Object> userClass = classRawdata.get(i);
			Map<String, Object> collectionGrade = new LinkedHashMap<String, Object>();
			boolean canContinue = true;
			if (userFilter && !String.valueOf(userClass.get("gooru_uid")).equalsIgnoreCase(requestParamsDTO.getFilters().getUserUId())) {
					canContinue = 	false;
			}
			if (canContinue) {
				collectionGrade.put("Student", userClass.get("username"));
				List<Map<String, Object>> resourceData = new ArrayList<Map<String, Object>>();
				Map<String, Object> where = new HashMap<String, Object>();
				where.put("collection_gooru_oid", requestParamsDTO.getFilters().getCollectionGooruOId());
				where.put("deleted", 0);
				resourceData = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), where, new ArrayList<String>()));
				requestParamsDTO.getFilters().setUserUId(userClass.get("gooru_uid").toString());
				String session = buildRowKey(traceId, requestParamsDTO);
				Map<String, Object> aggData = new HashMap<String, Object>();
				aggData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session));

				resourceData = getBaseService().sortBy(resourceData, "item_sequence", "ASC");
				/**
				 * fetch data for every collection
				 */
				for (Map<String, Object> singleResource : resourceData) {

					Map<String, Object> questionRawData = new HashMap<String, Object>();
					questionRawData = getBaseService().getColumnValue(traceId,
							getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), singleResource.get("resource_gooru_oid").toString(), resourceColumns));

					String resourceType = "Resource";
					if (questionRawData.get("type_name") != null && String.valueOf(questionRawData.get("type_name")).equalsIgnoreCase("assessment-question")) {
						resourceType = "Question";
					}

					String title = questionRawData.get("title") != null ? String.valueOf(questionRawData.get("title")).replaceAll(REGEXP, REPLACE) : "-";
					if (singleResource.containsKey("question_type")) {
						if (String.valueOf(singleResource.get("question_type")).equalsIgnoreCase("OE") || String.valueOf(singleResource.get("question_type")).equalsIgnoreCase("FIB")) {
							String text = "-";
							try {
								text = aggData.get(singleResource.get("resource_gooru_oid") + "~choice") == null ? "-" : URLDecoder.decode(
										String.valueOf(aggData.get(singleResource.get("resource_gooru_oid") + "~choice")), "UTF-8");
							} catch (Exception e) {
								text = aggData.get(singleResource.get("resource_gooru_oid") + "~choice") == null ? "-" : String.valueOf(aggData.get(singleResource.get("resource_gooru_oid")
										+ "~choice"));
							}
							collectionGrade.put("[" + resourceType + "]" + title + "-Answer", text == null ? "-" : text);

						} else if (String.valueOf(singleResource.get("question_type")).equalsIgnoreCase("MA")) {
							String value = aggData.get(singleResource.get("resource_gooru_oid") + "~choice") == null ? "-" : String.valueOf(aggData.get(singleResource.get("resource_gooru_oid")
									+ "~choice"));
							value = value.replaceAll("0", "NO");
							value = value.replaceAll("1", "YES");
							collectionGrade.put("[" + resourceType + "]" + title + "-Answer", value);
						} else {
							collectionGrade.put("[" + resourceType + "]" + title + "-Answer",
									aggData.get(singleResource.get("resource_gooru_oid") + "~options") == null ? "-" : aggData.get(singleResource.get("resource_gooru_oid") + "~options"));
						}
					} else {
						collectionGrade.put("[" + resourceType + "]" + title + "-Answer",
								aggData.get(singleResource.get("resource_gooru_oid") + "~options") == null ? "-" : aggData.get(singleResource.get("resource_gooru_oid") + "~options"));
					}
					collectionGrade.put(
							"[" + resourceType + "]" + title + "-Time Spent",
							aggData.get(singleResource.get("resource_gooru_oid") + "~time_spent") == null ? "-" : this.getHourlyBasedTimespent(Long.valueOf(String.valueOf(aggData.get(singleResource
									.get("resource_gooru_oid") + "~time_spent")))));
					String reaction = reactionMap.get(aggData.get(singleResource.get("resource_gooru_oid") + "~RA") == null ? "10" : aggData.get(singleResource.get("resource_gooru_oid") + "~RA")
							.toString());
					collectionGrade.put("[" + resourceType + "]" + title + "-Reaction", reaction);
				}

				collectionGrade.put(
						"[Collection]" + collectionRawData.get("title") + "-Total Time Spent",
						aggData.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~time_spent") == null ? "-" : this.getHourlyBasedTimespent(Long.valueOf(String.valueOf(aggData.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~time_spent")))));
				collectionGrade.put("[Collection]" + collectionRawData.get("title") + "-views",
						aggData.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~views") == null ? "-" : (Long) aggData.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~views"));
				String reaction = reactionMap.get(aggData.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~avg_reaction") == null ? "10" : aggData.get(
						requestParamsDTO.getFilters().getCollectionGooruOId() + "~avg_reaction").toString());
				collectionGrade.put("[Collection]" + collectionRawData.get("title") + "-Avg Reaction", reaction);

			} else {
				String userName = userClass.get("username") == null ? "-" : (String) userClass.get("username");
				classStudents.add(userName);
			}
			if (!collectionGrade.isEmpty()) {
				resourcesInfo.add(collectionGrade);
				classGrade.add(collectionGrade);
			}

			if (!classStudents.isEmpty()) {
				for (String student : classStudents) {
					Map<String, Object> value = new HashMap<String, Object>();
					value.put("Student", student);
					resourcesInfo.add(value);
					classGrade.add(value);
				}
			}
		}
		getBaseService().sortBy(classGrade, "Student", "ASC");
		if (!reportType.equalsIgnoreCase("json")) {
			Map<String, Object> files = new HashMap<String, Object>();
			String fileName = null;
			try {
				fileName = excelBuilderService.exportXlsReport(traceId, classInfo, "Student-Summary" + "." + reportType, true);
				fileName = excelBuilderService.exportXlsReport(traceId, collectionInfo, "Student-Summary" + "." + reportType, false);
				fileName = excelBuilderService.exportXlsReport(traceId, resourcesInfo, "Student-Summary" + "." + reportType, false);
			} catch (ParseException e) {
				InsightsLogger.error(traceId, e);
			} catch (IOException e) {
				InsightsLogger.error(traceId, e);
			}
			files.put("file", fileName);
			classGrade = new ArrayList<Map<String, Object>>();
			classGrade.add(files);
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		} else {
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		}
	}

	public ResponseParamDTO<Map<String, Object>> getExportProgress(String traceId, String classId, String data, String reportType, String timeZone) throws Exception {

		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> classGrade = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> collectionInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> collectionOverView = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> resourceInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> questionInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> classRawdata = new ArrayList<Map<String, Object>>();
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		requestParamsDTO.getFilters().setClassId(classId);

		if (!getBaseService().notNull(requestParamsDTO.getFilters().getCollectionGooruOId())) {
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		}
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add("title");
		resourceColumns.add("createdOn");
		resourceColumns.add("resourceType");

		Map<String, Object> classData = new HashMap<String, Object>();
		classData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), classId, resourceColumns));
		Map<String, Object> whereCondition = new HashMap<String, Object>();
		
		whereCondition.put("classpage_gooru_oid", classId);
		whereCondition.put("deleted", 0);
		whereCondition.put("is_group_owner", 0);
		classRawdata = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.CLASSPAGE.getColumnFamily(), whereCondition, new ArrayList<String>()));

		whereCondition = new HashMap<String, Object>();
		whereCondition.put("collection_gooru_oid", classId);
		whereCondition.put("deleted", 0);
		List<Map<String, Object>> collectionData = getBaseService().getRowsColumnValues(traceId,
				getCassandraService().readAll(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), whereCondition, new ArrayList<String>()));

		if (collectionData.isEmpty()) {
			responseParamDTO.setContent(collectionData);
			return responseParamDTO;
		}

		collectionData = getBaseService().sortBy(collectionData, "item_sequence", "ASC");
		Map<String, Object> classMapInfo = new LinkedHashMap<String, Object>();
		classMapInfo.put("Class Title", classData.get("title") != null ? String.valueOf(classData.get("title")).replaceAll(REGEXP, REPLACE) : "-");
		
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
			Date utcDate = new Date(Long.valueOf(classData.get("createdOn").toString()));
			Date clientDate = getBaseService().convertTimeZone(utcDate, "UTC", timeZone);
			String date = formatter.format(clientDate);
			classMapInfo.put("Class Date Created", date);
		} catch (Exception e) {
			InsightsLogger.error(traceId, e);
		}
		
		classInfo.add(classMapInfo);
		classGrade.add(classMapInfo);
		
		Map<String, Object> collectionRawData = new HashMap<String, Object>();
		collectionRawData = getBaseService()
				.getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), requestParamsDTO.getFilters().getCollectionGooruOId(), resourceColumns));
		Map<String, Object> collectionMapInfo = new LinkedHashMap<String, Object>();
		collectionMapInfo.put("Collection Title", collectionRawData.get("title") != null ? collectionRawData.get("title").toString().replaceAll(REGEXP, REPLACE) : "-");
		
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
			Date utcDate = new Date(Long.valueOf(collectionRawData.get("createdOn").toString()));
			Date clientDate = getBaseService().convertTimeZone(utcDate, "UTC", timeZone);
			String date = formatter.format(clientDate);
			collectionMapInfo.put("Collection Date Created", date);
		} catch (Exception e) {
			InsightsLogger.error(traceId, e);
		}
		
		collectionInfo.add(collectionMapInfo);
		classGrade.add(collectionMapInfo);
		Map<String, Object> collectionMapAgg = new LinkedHashMap<String, Object>();
		Map<String, Object> aggDataWithOutUser = new HashMap<String, Object>();
		
		String sessionName = requestParamsDTO.getFilters().getSession();
		requestParamsDTO.getFilters().setSession("AS");
		String session = buildRowKey(traceId, requestParamsDTO);
		requestParamsDTO.getFilters().setSession(sessionName);
		
		aggDataWithOutUser = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session));
		if (aggDataWithOutUser.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~question_count") != null
				&& aggDataWithOutUser.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~views") != null) {
		}
		Map<String, String> reactionMap = new HashMap<String, String>();
		reactionMap.put("1", "I need help");
		reactionMap.put("2", "I dont understand");
		reactionMap.put("3", "meh");
		reactionMap.put("4", "I understand");
		reactionMap.put("5", "I can explain");
		reactionMap.put("10", "-");
		collectionMapAgg.put("Collection Overview", "");
		collectionMapAgg.put(
				"Views",
				aggDataWithOutUser.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~views") == null ? "-" : aggDataWithOutUser.get(requestParamsDTO.getFilters().getCollectionGooruOId()
						+ "~views"));
		collectionMapAgg.put(
				"Total Time Spent",
				aggDataWithOutUser.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~time_spent") == null ? "-" : this.getHourlyBasedTimespent(Long.valueOf(String
						.valueOf(aggDataWithOutUser.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~time_spent")))));
		String reaction = reactionMap.get((aggDataWithOutUser.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~avg_reaction") == null ? "10" : aggDataWithOutUser.get(
				requestParamsDTO.getFilters().getCollectionGooruOId() + "~avg_reaction").toString()));
		collectionMapAgg.put("Avg Reaction", reaction);
		collectionMapAgg.put(
				"Avg Time Spent",
				aggDataWithOutUser.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~avg_time_spent") == null ? "-" : this.getHourlyBasedTimespent(Long.valueOf(String
						.valueOf(aggDataWithOutUser.get(requestParamsDTO.getFilters().getCollectionGooruOId() + "~avg_time_spent")))));
		collectionOverView.add(collectionMapAgg);
		classGrade.add(collectionMapAgg);
		for (Map<String, Object> userClass : classRawdata) {

			Map<String, Object> questionMapInfo = new LinkedHashMap<String, Object>();
			Map<String, Object> resourceMapInfo = new LinkedHashMap<String, Object>();
			questionMapInfo.put("Question Data", "");
			questionMapInfo.put("Student", userClass.get("username"));
			resourceMapInfo.put("Resource Data", "");
			resourceMapInfo.put("Student", userClass.get("username"));
			List<Map<String, Object>> resourceData = new ArrayList<Map<String, Object>>();
			Map<String, Object> condition = new HashMap<String, Object>();
			condition.put("collection_gooru_oid", requestParamsDTO.getFilters().getCollectionGooruOId());
			condition.put("deleted", 0);
			resourceData = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), condition, new ArrayList<String>()));
			Map<String, Object> aggData = new HashMap<String, Object>();
			requestParamsDTO.getFilters().setUserUId(userClass.get("gooru_uid").toString());
			session = buildRowKey(traceId, requestParamsDTO);
			aggData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session));
			resourceData = getBaseService().sortBy(resourceData, "item_sequence", "ASC");
			int i = 1, j = 1;
			for (Map<String, Object> singleResource : resourceData) {
				Map<String, Object> questionRawData = new HashMap<String, Object>();
				questionRawData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), "" + singleResource.get("resource_gooru_oid"), resourceColumns));
				if ((questionRawData.get("type_name") != null && questionRawData.get("type_name").toString().equalsIgnoreCase("assessment-question"))
						|| (questionRawData.get("resourceType") != null && questionRawData.get("resourceType").toString().equalsIgnoreCase("assessment-question"))) {

					String answer = (aggData.get(singleResource.get("resource_gooru_oid") + "~options") == null ? "-" : (String) aggData.get(singleResource.get("resource_gooru_oid") + "~options"));
					String score = "-";
					if (aggData.get(singleResource.get("resource_gooru_oid") + "~score") == null)
						score = "-";

					if (aggData.get(singleResource.get("resource_gooru_oid") + "~score") != null)
						score = String.valueOf(aggData.get(singleResource.get("resource_gooru_oid") + "~score"));

					if (score != null && !score.equalsIgnoreCase("-") && !answer.equalsIgnoreCase("skipped") && !score.equalsIgnoreCase("0"))
						score = "Correct";

					if (score != null && score.equalsIgnoreCase("0"))
						score = "InCorrect";

					if (answer.equalsIgnoreCase("skipped"))
						score = "-";

					if (singleResource.get("question_type").toString().equalsIgnoreCase("OE")) {
						score = "-";
						String text = "-";
						try {
							text = aggData.get(singleResource.get("resource_gooru_oid") + "~choice") == null ? "-" : URLDecoder.decode(
									String.valueOf(aggData.get(singleResource.get("resource_gooru_oid") + "~choice")), "UTF-8");
						} catch (Exception e) {
							text = aggData.get(singleResource.get("resource_gooru_oid") + "~choice") == null ? "-" : String.valueOf(aggData.get(singleResource.get("resource_gooru_oid") + "~choice"));
						}
						answer = text;
					}
					questionMapInfo.put("Question" + i + "- Answer", answer);
					questionMapInfo.put("Question" + i + "- Correct / Incorrect", score);
					questionMapInfo.put(
							"Question" + i + "- Time Spent",
							aggData.get(singleResource.get("resource_gooru_oid") + "~time_spent") == null ? "-" : this.getHourlyBasedTimespent(Long.valueOf(String.valueOf(aggData.get(singleResource
									.get("resource_gooru_oid") + "~time_spent")))));
					reaction = reactionMap
							.get((aggData.get(singleResource.get("resource_gooru_oid") + "~RA") == null ? "10" : aggData.get(singleResource.get("resource_gooru_oid") + "~RA").toString()));
					questionMapInfo.put("Question" + i + "- Attempts",
							aggData.get(singleResource.get("resource_gooru_oid") + "~attempts") == null ? "-" : aggData.get(singleResource.get("resource_gooru_oid") + "~attempts"));
					questionMapInfo.put("Question" + i + "- Reaction", reaction);
					i++;
				} else {
					
					resourceMapInfo.put("Resource" + j + "-Views",
							aggData.get(singleResource.get("resource_gooru_oid") + "~views") == null ? "-" : aggData.get(singleResource.get("resource_gooru_oid") + "~views"));
					resourceMapInfo.put(
							"Resource" + j + "-Time Spent",
							aggData.get(singleResource.get("resource_gooru_oid") + "~time_spent") == null ? "-" : this.getHourlyBasedTimespent(Long.valueOf(String.valueOf(aggData.get(singleResource
									.get("resource_gooru_oid") + "~time_spent")))));
					reaction = reactionMap
							.get((aggData.get(singleResource.get("resource_gooru_oid") + "~RA") == null ? "10" : aggData.get(singleResource.get("resource_gooru_oid") + "~RA").toString()));
					resourceMapInfo.put("Resource" + j + "-Reaction", reaction);
					j++;
				}
			}
			questionInfo.add(questionMapInfo);
			classGrade.add(questionMapInfo);
			if (!resourceMapInfo.isEmpty()) {
				resourceInfo.add(resourceMapInfo);

			}
		}
		if (!resourceInfo.isEmpty()) {
			resourceInfo = getBaseService().sortBy(resourceInfo, "Student", "ASC");
			classGrade.addAll(resourceInfo);
		}
		if (!reportType.equalsIgnoreCase("json")) {

			Map<String, Object> files = new HashMap<String, Object>();
			String fileName = null;
			try {
				fileName = excelBuilderService.exportXlsReport(traceId, classInfo, "Student-Progress" + "." + reportType, true);
				fileName = excelBuilderService.exportXlsReport(traceId, collectionInfo, "Student-Progress" + "." + reportType, false);
				fileName = excelBuilderService.exportXlsReport(traceId, collectionOverView, "Student-Progress" + "." + reportType, false);
				fileName = excelBuilderService.exportXlsReport(traceId, questionInfo, "Student-Progress" + "." + reportType, false);
				fileName = excelBuilderService.exportXlsReport(traceId, resourceInfo, "Student-Progress" + "." + reportType, false);
			} catch (ParseException e) {
				InsightsLogger.error(traceId, e);
			} catch (IOException e) {
				InsightsLogger.error(traceId, e);
			}
			files.put("file", fileName);
			classGrade = new ArrayList<Map<String, Object>>();
			classGrade.add(files);
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		} else {
			responseParamDTO.setContent(classGrade);
			return responseParamDTO;
		}
	}

	public ResponseParamDTO<Map<String, Object>> getClassProgress(String traceId, String classId, String data) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Collection<String> staticColumns = new ArrayList<String>();
		List<Map<String, Object>> classesData = new ArrayList<Map<String, Object>>();
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		requestParamsDTO.getFilters().setClassId(classId);
		staticColumns.add("title");
		staticColumns.add("createdOn");
		staticColumns.add("resourceType");
		
		getBaseService().existsFilter(requestParamsDTO);
		if (getBaseService().notNull(requestParamsDTO.getFilters().getSession())) {
			throw new BadRequestException(ErrorMessages.E103 + "Session can't be null");
		}
		if (getBaseService().notNull(requestParamsDTO.getFilters().getUserUId())) {
			throw new BadRequestException(ErrorMessages.E103 + " UserId can't be null");
		}
		
		Map<String, Object> classRawData = new HashMap<String, Object>();
		classRawData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), classId, staticColumns));
		List<Map<String, Object>> collectionData = getBaseService().getRowsColumnValues(traceId,
				getCassandraService().getClassPageResouceUsage(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), "collection_gooru_oid", classId));

		if (collectionData.isEmpty()) {
			responseParamDTO.setContent(collectionData);
			return responseParamDTO;
		}

		String collectionId = getBaseService().exportData(collectionData, ApiConstants.RESOURCEGOORUOID).toString();

		for (String id : collectionId.split(",")) {
		
			Map<String, Object> collection = new LinkedHashMap<String, Object>();
			Map<String, Object> collectionRawData = new HashMap<String, Object>();
			
			collectionRawData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), id, staticColumns));
			Map<String, Object> aggData = new HashMap<String, Object>();
			requestParamsDTO.getFilters().setCollectionGooruOId(id);
			String session = buildRowKey(traceId, requestParamsDTO);
			
			aggData = getBaseService().getColumnValue(traceId, getCassandraService().read(traceId, ColumnFamily.REAL_TIME_DASHBOARD.getColumnFamily(), session));
			collection.put("ClassTitle", classRawData.get("title"));
			collection.put("CollectionTitle", collectionRawData.get("title"));
			collection.put("TimeSpent", aggData.get(id + "~time_spent"));
			collection.put("TotalQuestions", aggData.get(id + "~question_count"));
			collection.put("Score", aggData.get(id + "~score"));
			classesData.add(collection);
		}
		responseParamDTO.setContent(classesData);
		return responseParamDTO;
	}

	/**
	 * 
	 * @param requestParamsDTO user provided input like session,classId,pathwayId,collectionId,userUId,sessionId
	 * @return	returns key to fetch the data depending upon the user requested input
	 * @throws Exception
	 */
	private String buildRowKey(String traceId, RequestParamsDTO requestParamsDTO) throws Exception{
		
		String id = null;
		StringBuffer idBuilder = new StringBuffer();
		String sessionId = ApiConstants.STRING_EMPTY;
		boolean hasSessionId = false;
		boolean hasClassId = false;
		boolean hasUserUId = false;
		
		if(getBaseService().notNull(requestParamsDTO.getFilters().getSession())){
			idBuilder.append(getSession(requestParamsDTO.getFilters().getSession()));
		} else{
			throw new BadRequestException(ErrorMessages.E104.replace(ApiConstants.REPLACER, ApiConstants.SESSION));
		}
		if (getBaseService().notNull(requestParamsDTO.getFilters().getSessionId())) {
			idBuilder.append(ApiConstants.TILDA);
			idBuilder.append(requestParamsDTO.getFilters().getSessionId());
			hasSessionId = true;
		}
		if (getBaseService().notNull(requestParamsDTO.getFilters().getClassId())) {
			idBuilder.append(ApiConstants.TILDA);
			idBuilder.append(requestParamsDTO.getFilters().getClassId());
			sessionId = requestParamsDTO.getFilters().getClassId();
			hasClassId = true;
		}
		if (getBaseService().notNull(requestParamsDTO.getFilters().getPathwayId())) {
			idBuilder.append(ApiConstants.TILDA);
			idBuilder.append(requestParamsDTO.getFilters().getPathwayId());
			sessionId +=ApiConstants.TILDA+requestParamsDTO.getFilters().getPathwayId();
		}
		if (getBaseService().notNull(requestParamsDTO.getFilters().getCollectionGooruOId())) {
			idBuilder.append(ApiConstants.TILDA);
			idBuilder.append(requestParamsDTO.getFilters().getCollectionGooruOId());
			sessionId +=ApiConstants.TILDA+requestParamsDTO.getFilters().getCollectionGooruOId();
		}
		if (getBaseService().notNull(requestParamsDTO.getFilters().getUserUId())) {
			idBuilder.append(ApiConstants.TILDA);
			idBuilder.append(requestParamsDTO.getFilters().getUserUId());
			hasUserUId = true;
		}
		
		id = idBuilder.toString();
		if(id.startsWith(ApiConstants.SessionAttributes.AS.getSession())){
			return id;
		}
		
		if(id.startsWith(ApiConstants.SessionAttributes.CS.getSession())){
			if(hasSessionId){
				return id.replace(ApiConstants.SessionAttributes.CS.getSession()+ApiConstants.TILDA,ApiConstants.STRING_EMPTY);
			}
			if(!hasUserUId && !hasSessionId){
				throw new BadRequestException(ErrorMessages.E103+ApiConstants.USERUID);
			}
			if(hasClassId && !hasSessionId){
				OperationResult<ColumnList<String>> recentSessionIds = getCassandraService().getClassPageUsage(
						traceId, ColumnFamily.MICRO_AGGREGATION.getColumnFamily(),ApiConstants.SessionAttributes.RS.getSession()+ApiConstants.TILDA, sessionId,new String(),
						getBaseService().convertStringToCollection(requestParamsDTO.getFilters().getUserUId()));
				ColumnList<String> columns = recentSessionIds.getResult();
				if(!columns.isEmpty()){
				sessionId = columns.getColumnByIndex(columns.size()-1).getStringValue();
				id = id.replace(ApiConstants.SessionAttributes.CS.getSession()+ApiConstants.TILDA, sessionId);
				}
			}else if(!hasClassId && !hasSessionId){
				OperationResult<ColumnList<String>> recentSessionIds = getCassandraService()
						.getClassPageUsage(traceId, ColumnFamily.MICRO_AGGREGATION.getColumnFamily(), ApiConstants.SessionAttributes.RS.getSession()+ApiConstants.TILDA, requestParamsDTO.getFilters().getCollectionGooruOId(), null,
								getBaseService().convertStringToCollection(requestParamsDTO.getFilters().getUserUId()));
				ColumnList<String> columns = recentSessionIds.getResult();
				if(!columns.isEmpty()){
				sessionId = columns.getColumnByIndex(columns.size()-1).getStringValue();
				id = id.replace(ApiConstants.SessionAttributes.CS.getSession()+ApiConstants.TILDA, sessionId);
				}
			}
			if(!getBaseService().notNull(sessionId)){
				InsightsLogger.error(traceId, ErrorMessages.E105);
			}
			return id;
		}
		if(id.startsWith(ApiConstants.SessionAttributes.FS.getSession())){
			if(!hasClassId && !hasUserUId){
				throw new BadRequestException(ErrorMessages.E103+ApiConstants.CLASSID_USERUID);
			}
			String rowKey = id.replace(ApiConstants.SessionAttributes.FS.getSession()+ApiConstants.TILDA, ApiConstants.STRING_EMPTY);
			OperationResult<ColumnList<String>> columnResult = getCassandraService().read(traceId, ColumnFamily.MICRO_AGGREGATION.getColumnFamily(), rowKey);
			 if (!columnResult.getResult().isEmpty()) {
		           List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
		           for (Column<String> column : columnResult.getResult()) {
		               Map<String, Object> sessionTimeStamp = new HashMap<String, Object>();
		               sessionTimeStamp.put("sessionId", column.getName());
		               try {
		                   Date date = secondsDateFormatter.parse(column.getStringValue());
		                   sessionTimeStamp.put("timeStamp", Math.abs(date.getTime()));
		               } catch (ParseException e) {
		            	   InsightsLogger.error(traceId, e);
		               }
		               dataList.add(sessionTimeStamp);
		           }
		           dataList = getBaseService().sortBy(dataList, "timeStamp", "DESC");
		           return id.replace(ApiConstants.SessionAttributes.FS.getSession(), dataList.get(0).get("sessionId").toString());
		       }else{
		    	   InsightsLogger.error(traceId, ErrorMessages.E105);
			}
		}
		return id;
	}
	
	private String getSession(String requestSession) throws Exception{
		Set<String> sessionList = new HashSet<String>();
		sessionList.add(ApiConstants.SessionAttributes.FS.getSession());
		sessionList.add(ApiConstants.SessionAttributes.AS.getSession());
		sessionList.add(ApiConstants.SessionAttributes.CS.getSession());
		requestSession = requestSession.toUpperCase();
		if (sessionList.contains(requestSession)) {
			return requestSession;
		}else{
			throw new BadRequestException(ErrorMessages.E103+ApiConstants.SESSION);
		}
	}

	private Map<String,Object> getItemCount(OperationResult<Rows<String, String>> collectionList){
		int counter = 0; 
		int totalquestionCount = 0;
		int itemCount = 0;
		int questionCount = 0;
		Map<String,Object> injuctableRecord = new HashMap<String, Object>();
		for (Row<String, String> row : collectionList.getResult()) {
			if (row.getColumns().size() > 0 && row.getColumns().getColumnByName("question_type") != null
					&& getBaseService().notNull(row.getColumns().getColumnByName("question_type").getStringValue())) {
				if(!row.getColumns().getColumnByName("question_type").getStringValue().equalsIgnoreCase("OE")){
					totalquestionCount++;
				}
				questionCount++;
			}else{
				counter++;
			}
			itemCount++;
		}
		injuctableRecord = new HashMap<String, Object>();
		injuctableRecord.put("resourceCount", counter);
		injuctableRecord.put("itemCount", itemCount);
		injuctableRecord.put("totalQuestionCount", totalquestionCount);
		injuctableRecord.put("nonResourceCount", questionCount);
		return injuctableRecord;
	}
	
	private String getHourlyBasedTimespent(double timeSpent) {

		long secs = Math.round(timeSpent / 1000);
		long hrs = (long) Math.floor(secs / 3600);
		long mins = (long) Math.floor((secs - (hrs * 3600)) / 60);
		long lsecs = (long) Math.floor(secs - (hrs * 3600) - (mins * 60));
		return ((hrs < 10) ? 0L + "" + hrs : hrs) + ":" + ((mins < 10) ? 0L + "" + mins : mins) + ":" + ((lsecs < 10) ? 0L + "" + lsecs : lsecs);
	}

	private BaseService getBaseService() {
		return baseService;
	}

	private SelectParamsService getSelectParamsService() {
		return selectParamsService;
	}

	private CassandraService getCassandraService() {
		return cassandraService;
	}

	@Override
	public ResponseParamDTO<Map<String, Object>> getExportReport(String traceId,
			String format, String classId, String reportType,
			String data, String timeZone, HttpServletResponse response)
			throws Exception {
		ResponseParamDTO<Map<String, Object>> dataReport = new ResponseParamDTO<Map<String, Object>>();
		if (reportType.equalsIgnoreCase("summary")) {
			dataReport = getExportSummary(traceId, classId, data, format, timeZone);
		} else if (reportType.equalsIgnoreCase("progress")) {
			dataReport = getExportProgress(traceId, classId, data, format, timeZone);
		} else if (reportType.equalsIgnoreCase("grade")) {
			dataReport = getExportGradeBook(traceId, classId, data, format, timeZone);
		} else if (reportType.equalsIgnoreCase("oe")) {
			dataReport = getClasspageCollectionOEResources(traceId, classId, data,
					format, timeZone);
		}
		return dataReport;
	}
	
	private List<Map<String,Object>> getAnswer(String traceId, List<Map<String,Object>> resultSet,Map<String,String> selectValues,String collectionId,boolean singleSession){
		Map<String, String> surName = new HashMap<String, String>();
		Collection<String> additionParameter = new ArrayList<String>();
		additionParameter.add("dataSet");
		surName.put("~A", "A");
		surName.put("~B", "B");
		surName.put("~C", "C");
		surName.put("~D", "D");
		surName.put("~E", "E");
		resultSet = getBaseService().buildJSON(traceId, resultSet,additionParameter, surName, singleSession);
		
			List<Map<String,Object>>		rawData = getBaseService().getData(
							getBaseService().getRowsColumnValues(traceId,
									getCassandraService().readAll(traceId, ColumnFamily.ASSESSMENT_ANSWER.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, collectionId,
											new ArrayList<String>())), ApiConstants.QUESTION_GOORU_OID);

					selectValues.remove("options");
					selectValues.put("options", "dataSet");
					resultSet = getBaseService().properNameEndsWith(getBaseService().LeftJoin(resultSet, rawData, ApiConstants.GOORUOID, ApiConstants.QUESTION_GOORU_OID), selectValues);
					return resultSet;
	}
}
