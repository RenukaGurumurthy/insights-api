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
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.utils.ValidationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
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

	public ResponseParamDTO<Map<String,Object>> getItemsUsage(String traceId, String baseKey, String userUid, boolean isUsageRequired,boolean isMetaRequired, boolean isSecure,String parentGooruIds) throws Exception {
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		for (String parentGooruId : parentGooruIds.split(COMMA)) {
			ColumnList<String> items = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), parentGooruId).getResult();
			for (Column<String> item : items) {
				Map<String, Object> dataMap = new HashMap<String, Object>();
				dataMap.put(GOORUOID, item.getName());
				dataMap.put(SEQUENCE, item.getIntegerValue());
				if(isMetaRequired){
					getResourceMetaData(dataMap, traceId, item.getName());
				}
				if(isUsageRequired){
					getClassUsageData(dataMap, traceId, getBaseService().appendTilda(baseKey,parentGooruId,item.getName(),userUid));
				}
				dataMapAsList.add(dataMap);
			}
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}
	
	
	public ResponseParamDTO<Map<String,Object>> getCourseUsage(String traceId, String classId, String courseId, String userUid, Boolean getUsageData, boolean isSecure) throws Exception {		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> rawDataMapAsList = new ArrayList<Map<String, Object>>();
		//fetch unit Ids
		OperationResult<ColumnList<String>> unitData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), courseId);
		ColumnList<String> units = unitData.getResult();
		long unitCount = units.size();
		StringBuffer unitGooruOids = new StringBuffer();
		for(String unit : units.getColumnNames()) {
			if(unitGooruOids.length() > 1) {
				unitGooruOids.append(COMMA);
			}
			unitGooruOids.append(unit);
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
			String courseKey = classId + ApiConstants.TILDA + courseId;
			List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();
			for(Map<String, Object> rawDataMap : rawDataMapAsList) {
				Map<String, Object> usageAsMap = new HashMap<String, Object>(1);
				Map<String, Object> usageDataAsMap = new HashMap<String, Object>(4);
				rawDataMap.put(ApiConstants.TYPE, ApiConstants.UNIT);
				String unitGooruOid = rawDataMap.get(ApiConstants.GOORUOID).toString();
				OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitGooruOid);
				ColumnList<String> lessons = lessonData.getResult();
				long lessonCount = lessons.size();
				usageDataAsMap.put(ApiConstants.COURSE_COUNT, unitCount);
				usageDataAsMap.put(ApiConstants.LESSON_COUNT, lessonCount);
				String classCourseKey = courseKey;
				//fetch course usage data
				if (StringUtils.isNotBlank(userUid)) {
					classCourseKey += ApiConstants.TILDA + userUid;
				}
				Map<String, Object> courseUsageAsMap = new HashMap<String, Object>();
				courseUsageAsMap = getActivityMetricsAsMap(traceId, classCourseKey, courseId);
				if(!courseUsageAsMap.isEmpty()) {
					usageDataAsMap.put(ApiConstants.COURSE_USAGE_DATA, courseUsageAsMap);
				}
				String classUnitKey = courseKey + ApiConstants.TILDA + unitGooruOid;
				//fetch unit usage data
				if (StringUtils.isNotBlank(userUid)) {
					classUnitKey += ApiConstants.TILDA + userUid;
				}
				Map<String, Object> unitUsageAsMap = new HashMap<String, Object>();
				unitUsageAsMap = getActivityMetricsAsMap(traceId, classUnitKey, unitGooruOid);
				if(!unitUsageAsMap.isEmpty()) {
					usageDataAsMap.put(ApiConstants.UNIT_USAGE_DATA, unitUsageAsMap);
				}
				usageAsMap.putAll(rawDataMap);
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
		List<Map<String, Object>> unitRawDataMapAsList = new ArrayList<Map<String, Object>>();

		//fetch lesson ids 
		OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitId);
		ColumnList<String> lessons = lessonData.getResult();
		long lessonCount = lessons.size();
		StringBuffer lessonGooruOids = new StringBuffer();
		for(Column<String> lesson : lessons) {
			if(lessonGooruOids.length() > 1) {
				lessonGooruOids.append(COMMA);
			}
			lessonGooruOids.append(lesson.getName());
		}
		
		//fetch metadata of unit
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCETYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		unitRawDataMapAsList = getResourceData(traceId, isSecure, unitRawDataMapAsList, unitId, resourceColumns);

		
		//fetch metadata of lessons
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, lessonGooruOids.toString(), resourceColumns);
		responseParamDTO.setContent(rawDataMapAsList);

		//fetch usage data of unit
		if(getUsageData) {
			String unitKey = classId + ApiConstants.TILDA +courseId + ApiConstants.TILDA + unitId;
			
			String classUnitKey = unitKey;
			//fetch unit's item views/attempts count
			if(StringUtils.isNotBlank(userUid)) {
				classUnitKey += ApiConstants.TILDA + userUid;
			}
			OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classUnitKey + ApiConstants.TILDA + ApiConstants.COLLECTION + ApiConstants.TILDA + ApiConstants.TIME_SPENT);
			long collectionsViewedInUnit = collectionMetricsData != null ? collectionMetricsData.getResult().size() : 0L;
			OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classUnitKey + ApiConstants.TILDA + ApiConstants.ASSESSMENT + ApiConstants.TILDA + ApiConstants.SCORE_IN_PERCENTAGE);
			long assessmentsAttemptedInUnit = assessmentMetricsData != null ? assessmentMetricsData.getResult().size() : 0L;
			
			List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> lessonResultMapList = new ArrayList<Map<String, Object>>();
			Map<String, Object> unitUsageDataAsMap = new HashMap<String, Object>(2);
			//fetch unit usage data
			Map<String, Object> unitUsageAsMap = new HashMap<String, Object>();
			unitUsageAsMap.put(ApiConstants.LESSON_COUNT, lessonCount);
			unitUsageAsMap.put(ApiConstants.COLLECTIONS_VIEWED, collectionsViewedInUnit);
			unitUsageAsMap.put(ApiConstants.ASSESSMENTS_ATTEMPTED, assessmentsAttemptedInUnit);
			unitUsageAsMap.putAll(getActivityMetricsAsMap(traceId, classUnitKey, unitId));
			if(!unitUsageAsMap.isEmpty()) {
				unitUsageDataAsMap.put(ApiConstants.UNIT_USAGE_DATA, unitUsageAsMap);
			}
			for(Map<String, Object> rawDataMap : rawDataMapAsList) {
				Map<String, Object> usageAsMap = new HashMap<String, Object>();
				String lessonGooruOid = rawDataMap.get(ApiConstants.GOORUOID).toString();
				OperationResult<ColumnList<String>> assessmentData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
				ColumnList<String> assessments = assessmentData.getResult();
				long assessmentCount = assessments.size();

				//fetch lesson usage data
				String classLessonKey = unitKey + ApiConstants.TILDA + lessonGooruOid;
				if (StringUtils.isNotBlank(userUid)) {
					classLessonKey += ApiConstants.TILDA + userUid;
				}
				Map<String, Object> lessonUsageAsMap = new HashMap<String, Object>();
				lessonUsageAsMap.put(ApiConstants.ASSESSMENT_COUNT, assessmentCount);
				lessonUsageAsMap.putAll(getActivityMetricsAsMap(traceId, classLessonKey, lessonGooruOid));
				
				usageAsMap.putAll(rawDataMap);
				if(!lessonUsageAsMap.isEmpty()) {
					usageAsMap.put(ApiConstants.USAGEDATA, lessonUsageAsMap);
				}
				lessonResultMapList.add(usageAsMap);
			}
			unitUsageDataAsMap.put(ApiConstants.LESSON, lessonResultMapList);
			if(!unitRawDataMapAsList.isEmpty() && unitRawDataMapAsList.size() > 0) {
				unitUsageDataAsMap.putAll(unitRawDataMapAsList.get(0));
			}
			resultMapList.add(unitUsageDataAsMap);
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
			OperationResult<ColumnList<String>>  classData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
			Long classMinScore = 0L;
			if(!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
				classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
			}
			String lessonKey = classId + ApiConstants.TILDA + courseId + ApiConstants.TILDA + unitId + ApiConstants.TILDA + lessonId;
			
			//fetch unit's item views/attempts count
			String classLessonKey = lessonKey;
			if(StringUtils.isNotBlank(userUid)) {
				classLessonKey += ApiConstants.TILDA + userUid;
			}
			OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey + ApiConstants.TILDA + ApiConstants.COLLECTION + ApiConstants.TILDA + ApiConstants.TIME_SPENT);
			long collectionsViewedInLesson = collectionMetricsData != null ? collectionMetricsData.getResult().size() : 0L;
			OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey + ApiConstants.TILDA + ApiConstants.ASSESSMENT + ApiConstants.TILDA + ApiConstants.SCORE_IN_PERCENTAGE);
			long assessmentsAttemptedInLesson = assessmentMetricsData != null ? assessmentMetricsData.getResult().size() : 0L;
			ColumnList<String> assessmentMetricColumnList = null ;
			if(assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
				assessmentMetricColumnList = assessmentMetricsData.getResult();
			}
			
			List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();
			OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitId);
			ColumnList<String> lessons = lessonData.getResult();
			long lessonCount = lessons.size();
			
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
				Map<String, Object> lessonUsageAsMap = new HashMap<String, Object>();
				lessonUsageAsMap = getActivityMetricsAsMap(traceId, classLessonKey, lessonId);
				if (!lessonUsageAsMap.isEmpty()) {
					usageDataAsMap.put(ApiConstants.LESSON_USAGE_DATA, lessonUsageAsMap);
				}

				// fetch collection usage data
				String classCollectionKey = lessonKey + itemGooruOid;
				if (StringUtils.isNotBlank(userUid)) {
					classCollectionKey += ApiConstants.TILDA + userUid;
				}
				Map<String, Object> collectionUsageAsMap = new HashMap<String, Object>();
				Long assessmentScore = null;
				if(assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
					assessmentScore = assessmentMetricColumnList.getLongValue(itemGooruOid, null);
					String scoreStatus = null;
					if(assessmentScore != null && assessmentScore >= classMinScore) {
						scoreStatus = ApiConstants.SCORE_MET;
					} else if(assessmentScore != null && assessmentScore < classMinScore) {
						scoreStatus = ApiConstants.SCORE_NOT_MET;
					} else if(assessmentScore == null) {
						scoreStatus = ApiConstants.NOT_ATTEMPTED;
					}
					collectionUsageAsMap.put(ApiConstants.SCORE_STATUS, scoreStatus);
				}
				collectionUsageAsMap.putAll(getActivityMetricsAsMap(traceId, classCollectionKey, itemGooruOid));
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
	
	public ResponseParamDTO<Map<String,Object>> getCoursePlanView(String traceId, String classId, String courseId, String userUid, boolean isSecure) throws Exception {
		if (StringUtils.isBlank(classId) || StringUtils.isBlank(courseId)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E108, baseService.addComma("classGooruId", "courseGooruId"));
		}
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> unitDataMapAsList = new ArrayList<Map<String, Object>>();
		OperationResult<ColumnList<String>>  classData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
		Long classMinScore = 0L;
		if(!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
			classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
		}
		OperationResult<ColumnList<String>> unitData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), courseId);
		ColumnList<String> units = unitData.getResult();
		for (Column<String> unit : units) {
			List<Map<String, Object>> lessonDataMapAsList = new ArrayList<Map<String, Object>>();
			Map<String, Object> unitDataAsMap = new HashMap<String, Object>();
			String unitGooruOid = unit.getName();
			Collection<String> resourceColumns = new ArrayList<String>();
			resourceColumns.add(ApiConstants.TITLE);
			resourceColumns.add(ApiConstants.GOORUOID);
			resourceColumns.add(ApiConstants.THUMBNAIL);
			OperationResult<ColumnList<String>>  unitMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), unitGooruOid, resourceColumns);
			if(!unitMetaData.getResult().isEmpty() && unitMetaData.getResult().size() > 0) {
				ColumnList<String> unitMetaDataColumns = unitMetaData.getResult();
				unitDataAsMap.put(ApiConstants.TITLE, unitMetaDataColumns.getColumnByName(ApiConstants.TITLE));
				unitDataAsMap.put(ApiConstants.GOORUOID, unitMetaDataColumns.getColumnByName(ApiConstants.GOORUOID));

				long scoreMet = 0;
				long scoreNotMet = 0;
				long attempted = 0;
				long notScored = 0;
				OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitGooruOid);
				ColumnList<String> lessons = lessonData.getResult();
				for (Column<String> lesson : lessons) {
					Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
					String lessonGooruOid = lesson.getName();
					String classLessonKey = classId + ApiConstants.TILDA + courseId + ApiConstants.TILDA + unitGooruOid + ApiConstants.TILDA + lessonGooruOid;
					if (StringUtils.isNotBlank(userUid)) {
						classLessonKey += ApiConstants.TILDA + userUid;
					}
					OperationResult<ColumnList<String>>  lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
					if(!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
						ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
						lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE));
						lessonDataAsMap.put(ApiConstants.GOORUOID, lessonMetaDataColumns.getColumnByName(ApiConstants.GOORUOID));

						OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
						ColumnList<String> items = itemData.getResult();
						for (Column<String> item : items) {
							String itemGooruOid = item.getName();
							Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
							OperationResult<ColumnList<String>>  itemMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), itemGooruOid, resourceColumns);
							if(!itemMetaData.getResult().isEmpty() && itemMetaData.getResult().size() > 0) {
								ColumnList<String> itemMetaDataColumns = itemMetaData.getResult();
								itemDataAsMap.put(ApiConstants.TITLE, itemMetaDataColumns.getColumnByName(ApiConstants.TITLE));
								itemDataAsMap.put(ApiConstants.GOORUOID, itemMetaDataColumns.getColumnByName(ApiConstants.GOORUOID));
							}
							OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey + ApiConstants.ASSESSMENT + ApiConstants.TILDA + ApiConstants.SCORE_IN_PERCENTAGE);
							ColumnList<String> assessmentMetricColumnList = null ;
							if(assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
								assessmentMetricColumnList = assessmentMetricsData.getResult();
							}
							Long assessmentScore = null;
							if(assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
								assessmentScore = assessmentMetricColumnList.getLongValue(itemGooruOid, null);
								if(assessmentScore != null && assessmentScore >= classMinScore) {
									scoreMet += 1;
									attempted += 1;
								} else if(assessmentScore != null && assessmentScore < classMinScore) {
									scoreNotMet += 1;
									attempted +=1;
									lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_NOT_MET);
									break;
								}
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
		responseParamDTO.setContent(unitDataMapAsList);
		return responseParamDTO;		
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getUnitPlanView(String traceId, String classId, String courseId, String unitId, String userUid, boolean isSecure)
 throws Exception {

		if (StringUtils.isBlank(classId) || StringUtils.isBlank(courseId)) {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E108, baseService.addComma("classGooruId", "courseGooruId"));
		}
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> lessonDataMapAsList = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
		OperationResult<ColumnList<String>> classData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
		Long classMinScore = 0L;
		if (!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
			classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
		}
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.GOORUOID);
		resourceColumns.add(ApiConstants.THUMBNAIL);

		long scoreMet = 0;
		long scoreNotMet = 0;
		long attempted = 0;
		long notScored = 0;
		OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitId);
		ColumnList<String> lessons = lessonData.getResult();
		for (Column<String> lesson : lessons) {
			Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
			String lessonGooruOid = lesson.getName();
			String classLessonKey = classId + ApiConstants.TILDA + courseId + ApiConstants.TILDA + unitId + ApiConstants.TILDA + lessonGooruOid;
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey += ApiConstants.TILDA + userUid;
			}
			OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
			if (!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
				ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
				lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE));
				lessonDataAsMap.put(ApiConstants.GOORUOID, lessonMetaDataColumns.getColumnByName(ApiConstants.GOORUOID));
				lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);

				OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
				ColumnList<String> items = itemData.getResult();
				for (Column<String> item : items) {
					String itemGooruOid = item.getName();
					Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
					OperationResult<ColumnList<String>>  itemMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
					if(!itemMetaData.getResult().isEmpty() && itemMetaData.getResult().size() > 0) {
						ColumnList<String> itemMetaDataColumns = itemMetaData.getResult();
						itemDataAsMap.put(ApiConstants.TITLE, itemMetaDataColumns.getColumnByName(ApiConstants.TITLE));
						itemDataAsMap.put(ApiConstants.GOORUOID, itemMetaDataColumns.getColumnByName(ApiConstants.GOORUOID));
						itemDataAsMap.put(ApiConstants.TYPE, itemMetaDataColumns.getColumnByName(ApiConstants.RESOURCETYPE));
					}
					
					OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
							classLessonKey + ApiConstants.TILDA + ApiConstants.ASSESSMENT + ApiConstants.TILDA + ApiConstants.SCORE_IN_PERCENTAGE);
					ColumnList<String> assessmentMetricColumnList = null;
					if (assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
						assessmentMetricColumnList = assessmentMetricsData.getResult();
					}
					Long assessmentScore = null;

					if (assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
						String assessmentScoreStatus = null;
						assessmentScore = assessmentMetricColumnList.getLongValue(itemGooruOid, null);
						if (assessmentScore != null && assessmentScore >= classMinScore) {
							scoreMet += 1;
							attempted += 1;
							assessmentScoreStatus = ApiConstants.SCORE_MET;
						} else if (assessmentScore != null && assessmentScore < classMinScore) {
							scoreNotMet += 1;
							attempted += 1;
							assessmentScoreStatus = ApiConstants.SCORE_NOT_MET;
							lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_NOT_MET);
							break;
						} else if (assessmentScore == null) {
							assessmentScoreStatus = ApiConstants.NOT_ATTEMPTED;
						}
						itemDataAsMap.put(ApiConstants.SCORE_STATUS, assessmentScoreStatus);
					}
					itemDataMapAsList.add(itemDataAsMap);
					lessonDataAsMap.put(ApiConstants.ITEM, itemDataMapAsList);
				}
				if (attempted == 0) {
					lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.NOT_ATTEMPTED);
				} else if (scoreMet > 0 && scoreNotMet == 0) {
					lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_MET);
				}
				lessonDataMapAsList.add(lessonDataAsMap);
			}
		}
		responseParamDTO.setContent(lessonDataMapAsList);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> getCourseProgressView(String traceId, String classId, String courseId, String userUid, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> unitDataMapAsList = new ArrayList<Map<String, Object>>();

		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.GOORUOID);
		
		OperationResult<ColumnList<String>> unitData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), courseId);
		ColumnList<String> units = unitData.getResult();
		if(!units.isEmpty() && units.size() > 0) {
			for (String unitGooruOid : units.getColumnNames()) {
				Map<String, Object> unitDataAsMap = new HashMap<String, Object>();

				//Form unit class key
				String unitKey = classId + ApiConstants.TILDA + courseId + ApiConstants.TILDA + unitGooruOid;
				String classUnitKey = unitKey;
				if (StringUtils.isNotBlank(userUid)) {
					classUnitKey += ApiConstants.TILDA + userUid;
				}
				//Fetch unit metadata
				OperationResult<ColumnList<String>> unitMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), unitGooruOid, resourceColumns);
				if (!unitMetaData.getResult().isEmpty() && unitMetaData.getResult().size() > 0) {
					ColumnList<String> unitMetaDataColumns = unitMetaData.getResult();
					unitDataAsMap.put(ApiConstants.TITLE, unitMetaDataColumns.getColumnByName(ApiConstants.TITLE));
					unitDataAsMap.put(ApiConstants.GOORUOID, unitMetaDataColumns.getColumnByName(ApiConstants.GOORUOID));
					unitDataAsMap.put(ApiConstants.TYPE, ApiConstants.UNIT);
				}
				long unitAssessmentCount = 0L;
				long unitCollectionCount = 0L;
				long unitAssessmentUrlCount = 0L;
				long unitItemCount = 0L;
				
				//Fetch lesson for item count details for this unit
				OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitGooruOid);
				ColumnList<String> lessons = lessonData.getResult();
				if(!lessons.isEmpty() && lessons.size() > 0) {
					StringBuffer collectionGooruOids = new StringBuffer();
					StringBuffer assessmentGooruOids = new StringBuffer();
					StringBuffer assessmentUrlGooruOids = new StringBuffer();
					StringBuffer itemGooruOids = new StringBuffer();
					for (String lessonGooruId : lessons.getColumnNames()) {
						// fetch item ids w.r.t their resource type
						getTypeBasedItemGooruOids(traceId, lessonGooruId, itemGooruOids, collectionGooruOids, assessmentGooruOids, assessmentUrlGooruOids);
						long assessmentCount = assessmentGooruOids.toString().split(ApiConstants.COMMA).length;
						long assessmentUrlCount = assessmentUrlGooruOids.toString().split(ApiConstants.COMMA).length;
						long collectionCount = collectionGooruOids.toString().split(ApiConstants.COMMA).length;
						long itemCount = itemGooruOids.toString().split(ApiConstants.COMMA).length;
						unitAssessmentCount += assessmentCount;
						unitCollectionCount += collectionCount;
						unitAssessmentUrlCount += assessmentUrlCount;
						unitItemCount += itemCount;
					}
				}
				
				//Fetch collections viewed count
				OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
						classUnitKey + ApiConstants.TILDA + ApiConstants.COLLECTION + ApiConstants.TILDA + ApiConstants.TIME_SPENT);
				ColumnList<String> collectionMetricColumnList = null;
				if (collectionMetricsData != null && !collectionMetricsData.getResult().isEmpty()) {
					collectionMetricColumnList = collectionMetricsData.getResult();
				}
				long collectionsViewed = 0L;
				collectionsViewed = collectionMetricColumnList.size();
				
				//Fetch collection study time
				long totalStudyTime = 0L;
				if (collectionMetricColumnList != null && collectionMetricColumnList.size() > 0) {
					for(Column<String> collectionMetricColumn : collectionMetricColumnList) {
						totalStudyTime += collectionMetricColumn.getLongValue();
					}
				}
				
				//Fetch assessments attempted count
				OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
						classUnitKey + ApiConstants.TILDA + ApiConstants.ASSESSMENT + ApiConstants.TILDA + ApiConstants.SCORE_IN_PERCENTAGE);
				ColumnList<String> assessmentMetricColumnList = null;
				if (assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
					assessmentMetricColumnList = assessmentMetricsData.getResult();
				}
				long assessmentAttempted = 0L;
				assessmentAttempted = assessmentMetricColumnList.size();
				
				//Fetch total of assessments score
				long totalScore = 0L;
				if (assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
					for(Column<String> assessmentMetricColumn : assessmentMetricColumnList) {
						totalScore += assessmentMetricColumn.getLongValue();
					}
				}
				
				unitDataAsMap.put(ApiConstants.ASSESSMENTS_ATTEMPTED, assessmentAttempted);
				unitDataAsMap.put(ApiConstants.COLLECTIONS_VIEWED, collectionsViewed);
				unitDataAsMap.put(ApiConstants.ASSESSMENT_COUNT, unitAssessmentCount);
				unitDataAsMap.put(ApiConstants.COLLECTION_COUNT, unitCollectionCount);
				unitDataAsMap.put(ApiConstants.TOTALSTUDYTIME, totalStudyTime);
				unitDataAsMap.put(ApiConstants.TOTALSCORE, totalScore);
				unitDataMapAsList.add(unitDataAsMap);
			}
			responseParamDTO.setContent(unitDataMapAsList);
		}
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> getUnitProgressView(String traceId, String classId, String courseId, String unitId, String userUid, boolean isSecure) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> resultDataMapAsList = new ArrayList<Map<String, Object>>();
		//Fetch minScore of class
		OperationResult<ColumnList<String>> classData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
		Long classMinScore = 0L;
		if (!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
			classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
		}
		
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.GOORUOID);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		
		OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitId);
		ColumnList<String> lessons = lessonData.getResult();		
		for (Column<String> lesson : lessons) {
			Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
			List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
			long scoreMet = 0;
			long scoreNotMet = 0;
			long attempted = 0;
			
			String lessonGooruOid = lesson.getName();
			String lessonKey = classId + ApiConstants.TILDA + courseId + ApiConstants.TILDA + unitId + ApiConstants.TILDA + lessonGooruOid;
			String classLessonKey = lessonKey;
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey += ApiConstants.TILDA + userUid;
			}
			OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
			if (!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
				ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
				lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE));
				lessonDataAsMap.put(ApiConstants.GOORUOID, lessonMetaDataColumns.getColumnByName(ApiConstants.GOORUOID));
				lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);

				//fetch item progress data
				OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
				ColumnList<String> items = itemData.getResult();
				for (Column<String> item : items) {
					Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
					String itemGooruOid = item.getName();
					// fetch item usage data
					itemDataAsMap.putAll(getClassActivityMetricsAsMap(traceId, classLessonKey, itemGooruOid));
					itemDataMapAsList.add(itemDataAsMap);

					//fetch lesson's score status data
					OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
							classLessonKey + ApiConstants.TILDA + ApiConstants.ASSESSMENT + ApiConstants.TILDA + ApiConstants.SCOREINPERCENTAGE);
					ColumnList<String> assessmentMetricColumnList = null;
					if (assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
						assessmentMetricColumnList = assessmentMetricsData.getResult();
					}
					Long assessmentScore = null;

					if (assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
						assessmentScore = assessmentMetricColumnList.getLongValue(itemGooruOid, null);
						if (assessmentScore != null && assessmentScore >= classMinScore) {
							scoreMet += 1;
							attempted += 1;
						} else if (assessmentScore != null && assessmentScore < classMinScore) {
							scoreNotMet += 1;
							attempted += 1;
							lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_NOT_MET);
							break;
						}
					}
				}
				lessonDataAsMap.put(ApiConstants.ITEM, itemDataMapAsList);
				if (attempted == 0) {
					lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.NOT_ATTEMPTED);
				} else if (scoreMet > 0 && scoreNotMet == 0) {
					lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_MET);
				}
				
			}
			if(!lessonDataAsMap.isEmpty() && lessonDataAsMap.size() > 0) {
				resultDataMapAsList.add(lessonDataAsMap);
			}
		}
		responseParamDTO.setContent(resultDataMapAsList);
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
	
	private Map<String, Object> getClassUsageData(Map<String, Object> dataMap, String traceId, String key) {
		ColumnList<String> resourceColumn = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key).getResult();
		for (Column<String> column : resourceColumn) {
			if (column.getName().contains("collection_type")) {
				dataMap.put(column.getName(), column.getStringValue());
			}else{
				dataMap.put(column.getName(), column.getStringValue());
			}
		}
		return dataMap;
	}
	
	private Map<String, Object> getResourceMetaData(Map<String, Object> dataMap, String traceId, String key) {
		// fetch metadata
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCETYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		ColumnList<String> resourceColumn = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), key, resourceColumns).getResult();
		for (Column<String> column : resourceColumn) {
			dataMap.put(column.getName(), column.getStringValue());
		}
		return dataMap;
	}
	private Map<String, Object> getClassMetaData(Map<String, Object> dataMap, String traceId, String key) {
		// fetch metadata
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCETYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		ColumnList<String> resourceColumn = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), key, resourceColumns).getResult();
		for (Column<String> column : resourceColumn) {
			dataMap.put(column.getName(), column.getStringValue());
		}
		return dataMap;
	}
	private Map<String, Object> getCollectionItemMetaData(Map<String, Object> dataMap, String traceId, String key) {
		// fetch metadata
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCETYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		ColumnList<String> resourceColumn = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), key, resourceColumns).getResult();
		for (Column<String> column : resourceColumn) {
			dataMap.put(column.getName(), column.getStringValue());
		}
		return dataMap;
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
				Long score = sessionMetricColumns.getLongValue(sessionKey + ApiConstants.TILDA + ApiConstants.SCOREINPERCENTAGE, 0L);
				usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
				usageAsMap.put(ApiConstants.VIEWS, views > 0 ? views : 0);
				usageAsMap.put(ApiConstants.TIME_SPENT, timeSpent > 0 ? timeSpent : 0);
				usageAsMap.put(ApiConstants.SCOREINPERCENTAGE, score > 0 ? score : 0);
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
			Long score = itemMetricColumns.getLongValue(contentGooruOid + ApiConstants.TILDA + ApiConstants.SCOREINPERCENTAGE, 0L);
			String collectionType = itemMetricColumns.getStringValue(contentGooruOid + ApiConstants.TILDA + ApiConstants.COLLECTION_TYPE, null);
			usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
			usageAsMap.put(ApiConstants.VIEWS, views);
			usageAsMap.put(ApiConstants.TIME_SPENT, timeSpent);
			usageAsMap.put(ApiConstants.SCOREINPERCENTAGE, score);
			usageAsMap.put(ApiConstants.COLLECTION_TYPE, collectionType);
			return usageAsMap;
		}
		return usageAsMap;
	}
	
	private Map<String, Object> getActivityMetricsAsMap(String traceId, String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if (!itemsColumnList.getResult().isEmpty()) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			Long views = itemMetricColumns.getLongValue(ApiConstants.VIEWS, 0L);
			Long timeSpent = itemMetricColumns.getLongValue(ApiConstants.TIME_SPENT, 0L);
			String collectionType = itemMetricColumns.getStringValue(ApiConstants.COLLECTION_TYPE, null);
			usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
			usageAsMap.put(ApiConstants.VIEWS, views);
			usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
			usageAsMap.put(ApiConstants.COLLECTIONTYPE, collectionType);
			return usageAsMap;
		}
		return usageAsMap;
	}
	
	private void getTypeBasedItemGooruOids(String traceId, String lessonId, StringBuffer itemGooruOids, StringBuffer collectionGooruOids, StringBuffer assessmentGooruOids, StringBuffer assessmentUrlGooruOids) {
		OperationResult<ColumnList<String>> contentItemRows = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonId);
		if (!contentItemRows.getResult().isEmpty()) {
			
			//fetch item ids w.r.t their resource type
			ColumnList<String> contentItems = contentItemRows.getResult();
			for (Column<String> item : contentItems) {
				String contentType = null;
				String itemGooruOid = item.getName();
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
