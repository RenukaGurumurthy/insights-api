package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.SessionAttributes;
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.utils.ValidationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class ClassServiceImpl implements ClassService, InsightsConstant {
	
	@Autowired
	private BaseService baseService;

	@Autowired
	private CassandraService cassandraService;
	
	@Resource
	private Properties filePath;

	private BaseService getBaseService() {
		return baseService;
	}

	private CassandraService getCassandraService() {
		return cassandraService;
	}

	public ResponseParamDTO<Map<String,Object>> getCourseUsage(String traceId, String classId, String courseId, String userUid, Boolean getUsageData, boolean isSecure) throws Exception {		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> rawDataMapAsList = new ArrayList<Map<String, Object>>();
		//fetch unit Ids
		OperationResult<Rows<String, String>> unitData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, courseId);
		Rows<String, String> units = unitData.getResult();
		long unitCount = units.size();
		StringBuffer unitGooruOids = new StringBuffer();
		for(Row<String, String> course : units) {
			if(unitGooruOids.length() > 1) {
				unitGooruOids.append(COMMA);
			}
			unitGooruOids.append(course.getColumns().getColumnByName(ApiConstants.RESOURCEGOORUOID));
		}
		//fetch unit metadata
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCETYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, unitGooruOids.toString(), resourceColumns);
		responseParamDTO.setContent(rawDataMapAsList);

		//fetch course usage data
		if(getUsageData) {
			String courseSessionKey = classId + ApiConstants.TILDA + courseId;
			List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();
			for(Map<String, Object> rawDataMap : rawDataMapAsList) {
				Map<String, Object> usageAsMap = new HashMap<String, Object>(1);
				Map<String, Object> usageDataAsMap = new HashMap<String, Object>(4);
				usageDataAsMap.putAll(rawDataMap);
				String unitGooruOid = rawDataMap.get(ApiConstants.GOORUOID).toString();
				OperationResult<Rows<String, String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, unitGooruOid);
				Rows<String, String> lessons = lessonData.getResult();
				long lessonCount = lessons.size();
				usageDataAsMap.put(ApiConstants.COURSE_COUNT, unitCount);
				usageDataAsMap.put(ApiConstants.LESSON_COUNT, lessonCount);
				String recentCourseSessionKey = SessionAttributes.RS + ApiConstants.TILDA + courseSessionKey;
				//fetch course usage data
				if (StringUtils.isNotBlank(userUid)) {
					recentCourseSessionKey += ApiConstants.TILDA + userUid;
				}
				Map<String, Object> courseUsageAsMap = new HashMap<String, Object>();
				courseUsageAsMap = getSessionMetricsAsMap(traceId, recentCourseSessionKey, courseId);
				if(!courseUsageAsMap.isEmpty()) {
					usageDataAsMap.put(ApiConstants.COURSE_USAGE_DATA, courseUsageAsMap);
				}
				String unitSessionKey = SessionAttributes.RS + ApiConstants.TILDA + courseSessionKey + ApiConstants.TILDA + unitGooruOid;
				//fetch unit usage data
				if (StringUtils.isNotBlank(userUid)) {
					unitSessionKey += ApiConstants.TILDA + userUid;
				}
				Map<String, Object> unitUsageAsMap = new HashMap<String, Object>();
				unitUsageAsMap = getSessionMetricsAsMap(traceId, unitSessionKey, unitGooruOid);
				if(!unitUsageAsMap.isEmpty()) {
					usageDataAsMap.put(ApiConstants.UNIT_USAGE_DATA, unitUsageAsMap);
				}
				usageAsMap.put(ApiConstants.USAGEDATA, usageDataAsMap);
				resultMapList.add(usageAsMap);
			}
			responseParamDTO.setContent(resultMapList);
		}
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> getUnitUsage(String traceId, String classId, String courseId, String unitId, String userUid, Boolean getUsageData, boolean isSecure) throws Exception {
		
		if(StringUtils.isBlank(classId) || StringUtils.isBlank(courseId) || StringUtils.isBlank(unitId)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E108, baseService.addComma("classGooruId","courseGooruId","unitGooruId"));
		}
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> rawDataMapAsList = new ArrayList<Map<String, Object>>();
		
		//fetch lesson ids 
		OperationResult<Rows<String, String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, unitId);
		Rows<String, String> lessons = lessonData.getResult();
		long lessonCount = lessons.size();
		StringBuffer lessonGooruOids = new StringBuffer();
		for(Row<String, String> lesson : lessons) {
			if(lessonGooruOids.length() > 1) {
				lessonGooruOids.append(COMMA);
			}
			lessonGooruOids.append(lesson.getColumns().getColumnByName(ApiConstants.RESOURCEGOORUOID));
		}
		
		//fetch metadata of lessons
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCETYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, lessonGooruOids.toString(), resourceColumns);
		responseParamDTO.setContent(rawDataMapAsList);

		//fetch usage data of unit
		if(getUsageData) {
			String unitSessionKey = classId + ApiConstants.TILDA +courseId + ApiConstants.TILDA + unitId;
			
			//fetch unit's item views/attempts count
			String classUnitKey = unitSessionKey;
			if(StringUtils.isNotBlank(userUid)) {
				classUnitKey += ApiConstants.TILDA + userUid;
			}
			OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classUnitKey + ApiConstants.COLLECTION);
			long collectionsViewedInUnit = collectionMetricsData != null ? collectionMetricsData.getResult().size() : 0L;
			OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classUnitKey + ApiConstants.COLLECTION);
			long assessmentsAttemptedInUnit = assessmentMetricsData != null ? assessmentMetricsData.getResult().size() : 0L;
			
			List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> lessonResultMapList = new ArrayList<Map<String, Object>>();
			Map<String, Object> unitUsageDataAsMap = new HashMap<String, Object>(2);
			String recentUnitSessionKey = SessionAttributes.RS + ApiConstants.TILDA + unitSessionKey;
			//fetch unit usage data
			if (StringUtils.isNotBlank(userUid)) {
				recentUnitSessionKey += ApiConstants.TILDA + userUid;
			}
			Map<String, Object> unitUsageAsMap = new HashMap<String, Object>();
			unitUsageAsMap.put(ApiConstants.LESSON_COUNT, lessonCount);
			unitUsageAsMap.put(ApiConstants.COLLECTIONS_VIEWED, collectionsViewedInUnit);
			unitUsageAsMap.put(ApiConstants.ASSESSMENTS_ATTEMPTED, assessmentsAttemptedInUnit);
			unitUsageAsMap.putAll(getSessionMetricsAsMap(traceId, recentUnitSessionKey, unitId));
			if(!unitUsageAsMap.isEmpty()) {
				unitUsageDataAsMap.put(ApiConstants.UNIT_USAGE_DATA, unitUsageAsMap);
			}
			resultMapList.add(unitUsageDataAsMap);
			for(Map<String, Object> rawDataMap : rawDataMapAsList) {
				Map<String, Object> usageAsMap = new HashMap<String, Object>();
				String lessonGooruOid = rawDataMap.get(ApiConstants.GOORUOID).toString();
				OperationResult<Rows<String, String>> assessmentData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, lessonGooruOid);
				Rows<String, String> assessments = assessmentData.getResult();
				long assessmentCount = assessments.size();

				//fetch lesson usage data
				String lessonSessionKey = SessionAttributes.RS + ApiConstants.TILDA + unitSessionKey + ApiConstants.TILDA + lessonGooruOid;
				if (StringUtils.isNotBlank(userUid)) {
					lessonSessionKey += ApiConstants.TILDA + userUid;
				}
				Map<String, Object> lessonUsageAsMap = new HashMap<String, Object>();
				lessonUsageAsMap.put(ApiConstants.ASSESSMENT_COUNT, assessmentCount);
				lessonUsageAsMap.putAll(getSessionMetricsAsMap(traceId, lessonSessionKey, lessonGooruOid));
				
				usageAsMap.putAll(rawDataMap);
				if(!lessonUsageAsMap.isEmpty()) {
					usageAsMap.put(ApiConstants.USAGEDATA, lessonUsageAsMap);
				}
				lessonResultMapList.add(usageAsMap);
			}
			unitUsageDataAsMap.put(ApiConstants.LESSON, lessonResultMapList);
			responseParamDTO.setContent(resultMapList);
		}
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String, Object>> getLessonUsage(String traceId, String classId, String courseId, String unitId, String lessonId, String userUid, Boolean getUsageData,
			boolean isSecure) throws Exception {

		if (StringUtils.isBlank(classId) || StringUtils.isBlank(courseId) || StringUtils.isBlank(unitId) || StringUtils.isBlank(lessonId)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E108, baseService.addComma("classGooruId", "courseGooruId", "unitGooruId", "lessonGooruId"));
		}
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> rawDataMapAsList = new ArrayList<Map<String, Object>>();

		// fetch item ids w.r.t their resource type
		StringBuffer collectionGooruOids = new StringBuffer();
		StringBuffer assessmentGooruOids = new StringBuffer();
		StringBuffer assessmentUrlGooruOids = new StringBuffer();
		StringBuffer itemGooruOids = new StringBuffer();
		getTypeBasedItemGooruOids(traceId, lessonId, itemGooruOids, collectionGooruOids, assessmentGooruOids, assessmentUrlGooruOids);
		long assessmentCount = assessmentGooruOids.toString().split(ApiConstants.COMMA).length;
		long assessmentUrlCount = assessmentUrlGooruOids.toString().split(ApiConstants.COMMA).length;
		long collectionCount = collectionGooruOids.toString().split(ApiConstants.COMMA).length;
		long itemCount = itemGooruOids.toString().split(ApiConstants.COMMA).length;

		// fetch metadata of items
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCETYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, itemGooruOids.toString(), resourceColumns);
		responseParamDTO.setContent(rawDataMapAsList);

		// fetch usage data
		if (getUsageData) {
			String lessonSessionKey = classId + ApiConstants.TILDA + courseId + ApiConstants.TILDA + unitId + ApiConstants.TILDA + lessonId;
			
			//fetch unit's item views/attempts count
			String classLessonKey = lessonSessionKey;
			if(StringUtils.isNotBlank(userUid)) {
				classLessonKey += ApiConstants.TILDA + userUid;
			}
			OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey + ApiConstants.COLLECTION);
			long collectionsViewedInLesson = collectionMetricsData != null ? collectionMetricsData.getResult().size() : 0L;
			OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey + ApiConstants.ASSESSMENT);
			long assessmentsAttemptedInLesson = assessmentMetricsData != null ? assessmentMetricsData.getResult().size() : 0L;
			ColumnList<String> assessmentMetricColumnList = null ;
			if(assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
				assessmentMetricColumnList = assessmentMetricsData.getResult();
			}
			
			List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();
			OperationResult<Rows<String, String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, unitId);
			Rows<String, String> lessons = lessonData.getResult();
			long lessonCount = lessons.size();
			
			String recentLessonSessionKey = SessionAttributes.RS + ApiConstants.TILDA + lessonSessionKey;
			for (Map<String, Object> rawDataMap : rawDataMapAsList) {
				Map<String, Object> usageAsMap = new HashMap<String, Object>(1);
				Map<String, Object> usageDataAsMap = new HashMap<String, Object>(4);
				String itemGooruOid = rawDataMap.get(ApiConstants.GOORUOID).toString();
				usageDataAsMap.put(ApiConstants.LESSON_COUNT, lessonCount);
				usageDataAsMap.put(ApiConstants.ITEM_COUNT, itemCount);
				usageDataAsMap.put(ApiConstants.ASSESSMENT_COUNT, assessmentCount);
				usageDataAsMap.put(ApiConstants.EXTERNAL_ASSESSMENT_COUNT, assessmentUrlCount);
				usageDataAsMap.put(ApiConstants.COLLECTION_COUNT, collectionCount);

				// fetch lesson usage data
				if (StringUtils.isNotBlank(userUid)) {
					recentLessonSessionKey += ApiConstants.TILDA + userUid;
				}
				Map<String, Object> lessonUsageAsMap = new HashMap<String, Object>();
				lessonUsageAsMap = getSessionMetricsAsMap(traceId, recentLessonSessionKey, lessonId);
				if (!lessonUsageAsMap.isEmpty()) {
					usageDataAsMap.put(ApiConstants.LESSON_USAGE_DATA, lessonUsageAsMap);
				}

				// fetch collection usage data
				String collectionSessionKey = SessionAttributes.RS + ApiConstants.TILDA + lessonSessionKey + itemGooruOid;
				if (StringUtils.isNotBlank(userUid)) {
					collectionSessionKey += ApiConstants.TILDA + userUid;
				}
				Map<String, Object> collectionUsageAsMap = new HashMap<String, Object>();
				//TODO get assessment's estimated score
				long estimatedScore = 50;
				long assessmentScore = 0;
				long assessmentAttemptCount = 0;
				if(assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
					assessmentScore = assessmentMetricColumnList.getLongValue(itemGooruOid + ApiConstants.TILDA + ApiConstants.SCORE, 0L);
					assessmentAttemptCount = assessmentMetricColumnList.getLongValue(itemGooruOid + ApiConstants.TILDA + ApiConstants.ATTEMPT_COUNT, 0L);
					String scoreStatus = null;
					if(assessmentAttemptCount > 0 && assessmentScore >= estimatedScore) {
						scoreStatus = ApiConstants.SCORE_MET;
					} else if(assessmentAttemptCount > 0 && assessmentScore < estimatedScore) {
						scoreStatus = ApiConstants.SCORE_NOT_MET;
					} else if(assessmentAttemptCount > 0 && assessmentScore == 0) {
						scoreStatus = ApiConstants.NOT_SCORED;
					} else if(assessmentAttemptCount == 0) {
						scoreStatus = ApiConstants.NOT_ATTEMPTED;
					}
					collectionUsageAsMap.put(ApiConstants.SCORE_STATUS, scoreStatus);
				}
				collectionUsageAsMap.putAll(getSessionMetricsAsMap(traceId, collectionSessionKey, itemGooruOid));
				if (!collectionUsageAsMap.isEmpty()) {
					usageDataAsMap.put(ApiConstants.COLLECTION_USAGE_DATA, collectionUsageAsMap);
				}
				usageAsMap.putAll(rawDataMap);
				usageAsMap.put(ApiConstants.USAGEDATA, usageDataAsMap);
				resultMapList.add(usageAsMap);
			}
			responseParamDTO.setContent(resultMapList);
		}
		return responseParamDTO;
	}
	
	//TODO Unit plan view
	public ResponseParamDTO<Map<String,Object>> getUnitPlanView(String traceId, String classId, String courseId, String unitId, String userUid, Boolean getUsageData, boolean isSecure) throws Exception {
		if (StringUtils.isBlank(classId) || StringUtils.isBlank(courseId) || StringUtils.isBlank(unitId)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E108, baseService.addComma("classGooruId", "courseGooruId", "unitGooruId"));
		}
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> unitDataMapAsList = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> lessonDataMapAsList = new ArrayList<Map<String, Object>>();
		
		OperationResult<Rows<String, String>> unitData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, courseId);
		Rows<String, String> units = unitData.getResult();
		for (Row<String, String> unit : units) {
			Map<String, Object> unitDataAsMap = new HashMap<String, Object>();
			String unitGooruOid = unit.getColumns().getColumnByName(ApiConstants.RESOURCEGOORUOID).getStringValue();
			Collection<String> resourceColumns = new ArrayList<String>();
			resourceColumns.add(ApiConstants.TITLE);
			resourceColumns.add(ApiConstants.GOORUOID);
			OperationResult<ColumnList<String>>  unitMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), unitGooruOid, resourceColumns);
			if(!unitMetaData.getResult().isEmpty() && unitMetaData.getResult().size() > 0) {
				ColumnList<String> unitMetaDataColumns = unitMetaData.getResult();
				unitDataAsMap.put(ApiConstants.TITLE, unitMetaDataColumns.getColumnByName(ApiConstants.TITLE));
				unitDataAsMap.put(ApiConstants.GOORUOID, unitMetaDataColumns.getColumnByName(ApiConstants.GOORUOID));

				long scoreMet = 0;
				long scoreNotMet = 0;
				long attempted = 0;
				long notScored = 0;
				OperationResult<Rows<String, String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, unitId);
				Rows<String, String> lessons = lessonData.getResult();
				for (Row<String, String> lesson : lessons) {
					Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
					String lessonGooruOid = lesson.getColumns().getColumnByName(ApiConstants.RESOURCEGOORUOID).getStringValue();
					String classLessonKey = classId + ApiConstants.TILDA + courseId + ApiConstants.TILDA + unitId + ApiConstants.TILDA + lessonGooruOid;
					if (StringUtils.isNotBlank(userUid)) {
						classLessonKey += ApiConstants.TILDA + userUid;
					}
					OperationResult<ColumnList<String>>  lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
					if(!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
						ColumnList<String> lessonMetaDataColumns = unitMetaData.getResult();
						lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE));
						lessonDataAsMap.put(ApiConstants.GOORUOID, lessonMetaDataColumns.getColumnByName(ApiConstants.GOORUOID));

						OperationResult<Rows<String, String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, lessonGooruOid);
						Rows<String, String> items = itemData.getResult();
						for (Row<String, String> item : items) {
							String itemGooruOid = item.getColumns().getColumnByName(ApiConstants.RESOURCEGOORUOID).getStringValue();
							String itemMinScore = item.getColumns().getColumnByName(ApiConstants.MINIMUM_SCORE).getStringValue();
							Long itemMinScoreLong = Long.valueOf(itemMinScore);
							
							OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey + ApiConstants.ASSESSMENT);
							ColumnList<String> assessmentMetricColumnList = null ;
							if(assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
								assessmentMetricColumnList = assessmentMetricsData.getResult();
							}
							long assessmentScore = 0;
							long assessmentAttemptCount = 0;
							
							if(assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
								assessmentScore = assessmentMetricColumnList.getLongValue(itemGooruOid + ApiConstants.TILDA + ApiConstants.SCORE, 0L);
								assessmentAttemptCount = assessmentMetricColumnList.getLongValue(itemGooruOid + ApiConstants.TILDA + ApiConstants.ATTEMPT_COUNT, 0L);
								if(assessmentAttemptCount > 0 && assessmentScore >= itemMinScoreLong) {
									scoreMet += 1;
								} else if(assessmentAttemptCount > 0 && assessmentScore < itemMinScoreLong) {
									scoreNotMet += 1;
									lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_NOT_MET);
									break;
								} else if(assessmentAttemptCount > 0 && assessmentScore == 0) {
									notScored += 1;
								} else if(assessmentAttemptCount > 0) {
									attempted += 1;
								}
							}
						}
						if(attempted == 0) {
							lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.NOT_ATTEMPTED);
						} else if (scoreMet > 0 && scoreNotMet == 0 && notScored == 0) {
							lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_MET);
						}
						lessonDataMapAsList.add(lessonDataAsMap);
					}
				}
				unitDataAsMap.put(ApiConstants.LESSON, lessonDataMapAsList);
				unitDataMapAsList.add(unitDataAsMap);
			}
		}
		responseParamDTO.setContent(unitDataMapAsList);
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> getResourceData(String traceId, boolean isSecure, List<Map<String, Object>> rawDataMapAsList, String keys, Collection<String> columnsToFetch) {
		rawDataMapAsList = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.RESOURCE.getColumnFamily(), null, keys, new String(), columnsToFetch));
		Map<String, Map<Integer, String>> combineMap = new HashMap<String, Map<Integer, String>>();
		Map<Integer, String> filterMap = new HashMap<Integer, String>();
		filterMap.put(0, filePath.getProperty(ApiConstants.NFS_BUCKET));
		filterMap.put(1, ApiConstants.FOLDER);
		filterMap.put(2, ApiConstants.THUMBNAIL);
		combineMap.put(ApiConstants.THUMBNAIL, filterMap);
		rawDataMapAsList = getBaseService().appendInnerData(rawDataMapAsList, combineMap, isSecure ? ApiConstants.HTTPS : ApiConstants.HTTP);
		rawDataMapAsList = getBaseService().addCustomKeyInMapList(rawDataMapAsList, ApiConstants.GOORUOID, null);
		return rawDataMapAsList;
	}
	
	private Map<String, Object> getSessionMetricsAsMap(String traceId, String sessionKey, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		OperationResult<ColumnList<String>> sessionColumnResult = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), sessionKey);
		if (!sessionColumnResult.getResult().isEmpty()) {
			String sessionId = sessionColumnResult.getResult().getColumnByName(ApiConstants.SESSIONID).getStringValue();
			OperationResult<ColumnList<String>> sessionMetricsColumnResult = getCassandraService().read(traceId, ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionId);
			if (!sessionMetricsColumnResult.getResult().isEmpty()) {
				ColumnList<String> sessionMetricColumns = sessionMetricsColumnResult.getResult();
				Long views = sessionMetricColumns.getLongValue(sessionKey + ApiConstants.TILDA + ApiConstants.VIEWS, 0L);
				Long timeSpent = sessionMetricColumns.getLongValue(sessionKey + ApiConstants.TILDA + ApiConstants.TIME_SPENT, 0L);
				Long score = sessionMetricColumns.getLongValue(sessionKey + ApiConstants.TILDA + ApiConstants.SCORE, 0L);
				Long attempts = sessionMetricColumns.getLongValue(sessionKey + ApiConstants.TILDA + ApiConstants.ATTEMPT_COUNT, 0L);
				usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
				usageAsMap.put(ApiConstants.VIEWS, views > 0 ? views : 0);
				usageAsMap.put(ApiConstants.TIME_SPENT, timeSpent > 0 ? timeSpent : 0);
				usageAsMap.put(ApiConstants.SCORE, score > 0 ? score : 0);
				usageAsMap.put(ApiConstants.ATTEMPT_COUNT, attempts > 0 ? attempts : 0);
				return usageAsMap;
			}
		}
		return usageAsMap;
	}
	
	private Map<String, Object> getClassActivityMetricsAsMap(String traceId, String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if (!itemsColumnList.getResult().isEmpty()) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			Long views = itemMetricColumns.getLongValue(contentGooruOid + ApiConstants.TILDA + ApiConstants.VIEWS, 0L);
			Long timeSpent = itemMetricColumns.getLongValue(contentGooruOid + ApiConstants.TILDA + ApiConstants.TIME_SPENT, 0L);
			Long score = itemMetricColumns.getLongValue(contentGooruOid + ApiConstants.TILDA + ApiConstants.SCORE, 0L);
			Long attempts = itemMetricColumns.getLongValue(contentGooruOid + ApiConstants.TILDA + ApiConstants.ATTEMPT_COUNT, 0L);
			usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
			usageAsMap.put(ApiConstants.VIEWS, views > 0 ? views : 0);
			usageAsMap.put(ApiConstants.TIME_SPENT, timeSpent > 0 ? timeSpent : 0);
			usageAsMap.put(ApiConstants.SCORE, score > 0 ? score : 0);
			usageAsMap.put(ApiConstants.ATTEMPT_COUNT, attempts > 0 ? attempts : 0);
			return usageAsMap;
		}
		return usageAsMap;
	}
	
	private void getTypeBasedItemGooruOids(String traceId, String lessonId, StringBuffer itemGooruOids, StringBuffer collectionGooruOids, StringBuffer assessmentGooruOids, StringBuffer assessmentUrlGooruOids) {
		OperationResult<Rows<String, String>> contentItemRows = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, lessonId);
		if (!contentItemRows.getResult().isEmpty()) {
			
			//fetch item ids w.r.t their resource type
			Rows<String, String> contentItems = contentItemRows.getResult();
			for (Row<String, String> item : contentItems) {
				String contentType = null;
				String itemGooruOid = item.getColumns().getColumnByName(ApiConstants.RESOURCEGOORUOID).getStringValue();
				ColumnList<String> resourceData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), itemGooruOid).getResult();
				if (!resourceData.isEmpty() && resourceData.size() > 0) {
					contentType = resourceData.getColumnByName(ApiConstants.RESOURCETYPE).getStringValue();
				}
				if (contentType != null) {
					if (contentType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
						if (collectionGooruOids.length() > 0) {
							collectionGooruOids.append(COMMA);
						}
						collectionGooruOids.append(itemGooruOid);
					} else if (contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT)) {
						if (assessmentGooruOids.length() > 0) {
							assessmentGooruOids.append(COMMA);
						}
						assessmentGooruOids.append(itemGooruOid);
					} else if (contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT_URL)) {
						if (assessmentUrlGooruOids.length() > 0) {
							assessmentUrlGooruOids.append(COMMA);
						}
						assessmentUrlGooruOids.append(itemGooruOid);
					}
				}
				if (itemGooruOids.length() > 0) {
					itemGooruOids.append(COMMA);
				}
				itemGooruOids.append(itemGooruOid);
			}
		}
	}
}
