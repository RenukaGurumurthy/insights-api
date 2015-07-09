package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.utils.InsightsLogger;
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
	
	public ResponseParamDTO<Map<String,Object>> getUnitUsage(String traceId, String classId, String courseId, String unitId, String userUid, String collectionType, Boolean getUsageData, boolean isSecure) throws Exception {
		
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
			}else{
				
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

				String lessonGooruOid = rawDataMap.get(ApiConstants.GOORUOID).toString();
				OperationResult<ColumnList<String>> assessmentData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
				ColumnList<String> assessments = assessmentData.getResult();
				Map<String, Object> usageAsMap = new HashMap<String, Object>();
				usageAsMap.put(ApiConstants.SEQUENCE, lessons.getIntegerValue(lessonGooruOid, 0));
				usageAsMap.put(ApiConstants.ASSESSMENT_COUNT, assessments.size());
				usageAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
				

				//fetch lesson usage data
				Map<String, Object> lessonUsageAsMap = new HashMap<String, Object>();
				String classLessonKey = unitKey + ApiConstants.TILDA + lessonGooruOid;
				if (StringUtils.isNotBlank(userUid)) {
					classLessonKey += ApiConstants.TILDA + userUid;
					lessonUsageAsMap.putAll(getActivityMetricsAsMap(traceId, classLessonKey, lessonGooruOid));
					
					usageAsMap.put(ApiConstants.USAGEDATA, lessonUsageAsMap);
				}else {
						/**
						 * Fetch the list of user usage data and store it in lessonUsageAsMap as userUsage
						 */
					List<Map<String,Object>> assessmentUsage = null;
					String contentType = null;
					if(collectionType !=null){
						if(collectionType.equalsIgnoreCase(ApiConstants.ASSESSMENT)){
							contentType = ApiConstants.ASSESMENT_TYPE_MATCHER;
						}else if(collectionType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
							contentType = ApiConstants.COLLECTION;
						}
					}
					List<Map<String,Object>> collections = getContentItems(traceId,lessonGooruOid,contentType,true);
					List<Map<String,Object>> students = getStudents(traceId,classId);
					if(!(students.isEmpty() || collections.isEmpty())){
						Set<String> columnSuffix = new HashSet<String>();
						columnSuffix.add(ApiConstants.VIEWS);
						columnSuffix.add(ApiConstants.TIME_SPENT);
						columnSuffix.add(ApiConstants.SCORE_IN_PERCENTAGE);
						StringBuffer studentIds = getBaseService().exportData(students, ApiConstants.USERUID);
						StringBuffer collectionIds = getBaseService().exportData(collections, ApiConstants.GOORUOID);
						Collection<String> rowKeys = getBaseService().appendAdditionalField(ApiConstants.TILDA,classLessonKey, studentIds.toString());
						Collection<String> columns = getBaseService().appendAdditionalField(ApiConstants.TILDA,collectionIds.toString(), columnSuffix);
						/**
						 * Get collection activity
						 */
						assessmentUsage = getCollectionActivityMetrics(traceId, rowKeys, columns, studentIds.toString());
						
						assessmentUsage = getBaseService().LeftJoin(assessmentUsage, students, ApiConstants.USERUID, ApiConstants.USERUID);
						assessmentUsage = getBaseService().groupDataDependOnkey(assessmentUsage,ApiConstants.GOORUOID,ApiConstants.USAGEDATA);
						assessmentUsage = getBaseService().LeftJoin(collections,assessmentUsage,ApiConstants.GOORUOID,ApiConstants.GOORUOID);
					}
					usageAsMap.put(ApiConstants.USAGEDATA, assessmentUsage);
				}
				
				usageAsMap.putAll(rawDataMap);
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
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E108, baseService.appendComma("classGooruId", "courseGooruId", "unitGooruId", "lessonGooruId"));
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
	
	public ResponseParamDTO<Map<String,Object>> getCoursePlan(String traceId, String classId, String courseId, String userUid, boolean isSecure) throws Exception {
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
				unitDataAsMap.put(ApiConstants.TITLE, unitMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
				unitDataAsMap.put(ApiConstants.GOORUOID, unitGooruOid);
				unitDataAsMap.put(ApiConstants.TYPE, ApiConstants.UNIT);

				long scoreMet = 0;
				long scoreNotMet = 0;
				long attempted = 0;
				long notScored = 0;
				OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitGooruOid);
				ColumnList<String> lessons = lessonData.getResult();
				for (Column<String> lesson : lessons) {
					Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
					String lessonGooruOid = lesson.getName();
					String classLessonKey = getBaseService().appendTilda(classId, courseId, unitGooruOid, lessonGooruOid);
					if (StringUtils.isNotBlank(userUid)) {
						classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
					}
					OperationResult<ColumnList<String>>  lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
					lessonDataAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
                    lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);
					if(!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
						ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
						lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE).toString());

						OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
						ColumnList<String> items = itemData.getResult();
						for (Column<String> item : items) {
							String itemGooruOid = item.getName();
							Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
							OperationResult<ColumnList<String>>  itemMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), itemGooruOid, resourceColumns);
							if(!itemMetaData.getResult().isEmpty() && itemMetaData.getResult().size() > 0) {
								ColumnList<String> itemMetaDataColumns = itemMetaData.getResult();
								itemDataAsMap.put(ApiConstants.TITLE, itemMetaDataColumns.getColumnByName(ApiConstants.TITLE).toString());
							}
							itemDataAsMap.put(ApiConstants.GOORUOID, itemGooruOid);
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
				unitDataAsMap.put(ApiConstants.ITEM, lessonDataMapAsList);
				unitDataMapAsList.add(unitDataAsMap);
			}
		responseParamDTO.setContent(unitDataMapAsList);
		return responseParamDTO;		
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getUnitPlan(String traceId, String classId, String courseId, String unitId, String userUid, boolean isSecure)
 throws Exception {

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
		OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitId);
		ColumnList<String> lessons = lessonData.getResult();
		for (Column<String> lesson : lessons) {
			Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
			String lessonGooruOid = lesson.getName();
			String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
			}
			OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
			if (!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
				ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
				lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
				lessonDataAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
				lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);

				OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
				ColumnList<String> items = itemData.getResult();
				for (Column<String> item : items) {
					String itemGooruOid = item.getName();
					Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
					
					List<Map<String, Object>> rawDataMapAsList = new ArrayList<Map<String, Object>>();
					rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, itemGooruOid, resourceColumns);
					if(rawDataMapAsList.size() > 0) {
						itemDataAsMap.putAll(rawDataMapAsList.get(0));
					}
					
					OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
							getBaseService().appendTilda(classLessonKey, ApiConstants.ASSESSMENT, ApiConstants.SCORE_IN_PERCENTAGE));
					ColumnList<String> assessmentMetricColumnList = null;
					if (assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
						assessmentMetricColumnList = assessmentMetricsData.getResult();
					}
					Long assessmentScore = null;
					String assessmentScoreStatus = null;

					if (assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
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
						}
					}
					if (assessmentScore == null) {
						assessmentScoreStatus = ApiConstants.NOT_ATTEMPTED;
					}
					itemDataAsMap.put(ApiConstants.SCORE_STATUS, assessmentScoreStatus);
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
	
	public ResponseParamDTO<Map<String,Object>> getCourseProgress(String traceId, String classId, String courseId, String userUid, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = null;
		if (StringUtils.isNotBlank(userUid)) {
			responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
			List<Map<String, Object>> unitDataMapAsList = new ArrayList<Map<String, Object>>();
			Collection<String> resourceColumns = new ArrayList<String>();
			resourceColumns.add(ApiConstants.TITLE);
			resourceColumns.add(ApiConstants.GOORUOID);

			OperationResult<ColumnList<String>> unitData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), courseId);
			ColumnList<String> units = unitData.getResult();
			if (!units.isEmpty() && units.size() > 0) {
				for (String unitGooruOid : units.getColumnNames()) {
					Map<String, Object> unitDataAsMap = new HashMap<String, Object>();

					// Form unit class key
					String classUnitKey = getBaseService().appendTilda(classId, courseId, unitGooruOid);
					if (StringUtils.isNotBlank(userUid)) {
						classUnitKey = getBaseService().appendTilda(classUnitKey, userUid);
					}
					// Fetch unit metadata
					OperationResult<ColumnList<String>> unitMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), unitGooruOid, resourceColumns);
					if (!unitMetaData.getResult().isEmpty() && unitMetaData.getResult().size() > 0) {
						ColumnList<String> unitMetaDataColumns = unitMetaData.getResult();
						unitDataAsMap.put(ApiConstants.TITLE, unitMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
					}
					unitDataAsMap.put(ApiConstants.GOORUOID, unitGooruOid);
					unitDataAsMap.put(ApiConstants.TYPE, ApiConstants.UNIT);
					long unitAssessmentCount = 0L;
					long unitCollectionCount = 0L;
					long unitAssessmentUrlCount = 0L;
					long unitItemCount = 0L;

					// Fetch lesson for item count details for this unit
					OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitGooruOid);
					ColumnList<String> lessons = lessonData.getResult();
					if (!lessons.isEmpty() && lessons.size() > 0) {
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

					// Fetch collections viewed count
					OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
							getBaseService().appendTilda(classUnitKey, ApiConstants.COLLECTION, ApiConstants.TIME_SPENT));
					ColumnList<String> collectionMetricColumnList = null;
					if (collectionMetricsData != null && !collectionMetricsData.getResult().isEmpty()) {
						collectionMetricColumnList = collectionMetricsData.getResult();
					}
					long collectionsViewed = 0L;
					if (collectionMetricColumnList != null) {
						collectionsViewed = collectionMetricColumnList.size();
					}

					// Fetch collection study time
					long totalStudyTime = 0L;
					if (collectionMetricColumnList != null && collectionMetricColumnList.size() > 0) {
						for (Column<String> collectionMetricColumn : collectionMetricColumnList) {
							totalStudyTime += collectionMetricColumn.getLongValue();
						}
					}

					// Fetch assessments attempted count
					OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
							getBaseService().appendTilda(classUnitKey, ApiConstants.ASSESSMENT, ApiConstants.SCORE_IN_PERCENTAGE));
					ColumnList<String> assessmentMetricColumnList = null;
					if (assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
						assessmentMetricColumnList = assessmentMetricsData.getResult();
					}
					long assessmentAttempted = 0L;
					if (assessmentMetricColumnList != null) {
						assessmentAttempted = assessmentMetricColumnList.size();
					}

					// Fetch total of assessments score
					long totalScore = 0L;
					float avgScore = 0;
					if (assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
						for (Column<String> assessmentMetricColumn : assessmentMetricColumnList) {
							totalScore += assessmentMetricColumn.getLongValue();
						}
						if (unitAssessmentCount != 0) {
							avgScore = totalScore / unitAssessmentCount;
						}
					}

					unitDataAsMap.put(ApiConstants.COLLECTIONS_VIEWED, collectionsViewed);
					unitDataAsMap.put(ApiConstants.TOTALSTUDYTIME, totalStudyTime);
					unitDataAsMap.put(ApiConstants.ASSESSMENTS_ATTEMPTED, assessmentAttempted);
					unitDataAsMap.put(ApiConstants.AVGSCORE, avgScore);
					unitDataAsMap.put(ApiConstants.ASSESSMENT_COUNT, unitAssessmentCount);
					unitDataAsMap.put(ApiConstants.COLLECTION_COUNT, unitCollectionCount);
					unitDataMapAsList.add(unitDataAsMap);
				}
				responseParamDTO.setContent(unitDataMapAsList);
			}
		}else{
			responseParamDTO = getAllStudentProgressByUnit(traceId, classId, courseId, isSecure);	
		}
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getUserSessions(String traceId, String classId, String courseId, String unitId, String lessonId, String collectionId, String collectionType,
			String userUid, boolean fetchOpenSession, boolean isSecure) throws Exception {
		String key = baseService.appendTilda(classId, courseId, unitId, lessonId, collectionId, userUid);
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		OperationResult<ColumnList<String>> sessions = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), key);
		List<Map<String, Object>> resultSet = null;
		if (sessions != null) {
			ColumnList<String> sessionList = sessions.getResult();
			if (!sessionList.isEmpty()) {
				resultSet = new ArrayList<Map<String, Object>>();
				if (!fetchOpenSession) {
					int sequence = 0;
					for (Column<String> sessionColumn : sessionList) {
						resultSet.add(generateSessionMap(sequence++,sessionColumn.getName(), sessionColumn.getLongValue()));
					}
				}else{
					ColumnList<String> sessionsInfo = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), baseService.appendTilda(key,INFO)).getResult();
					for (Column<String> sessionColumn : sessionList) {
						int sequence = 0;
						if(sessionsInfo.getStringValue(baseService.appendTilda(sessionColumn.getName(),TYPE), null).equalsIgnoreCase(START)){
							resultSet.add(generateSessionMap(sequence++,sessionColumn.getName(), sessionColumn.getLongValue()));	
						}
					}
				}
				if(resultSet != null){
					responseParamDTO.setContent(baseService.sortBy(resultSet, EVENT_TIME, ApiConstants.ASC));
				}
			}
		}
		return responseParamDTO;
	}
	
	private Map<String,Object> generateSessionMap(int sequence,String sessionId,Long eventTime){
		HashMap<String, Object> session = new HashMap<String, Object>();
		session.put(SEQUENCE, sequence);
		session.put(SESSION_ID, sessionId);
		session.put(EVENT_TIME, eventTime);
		return session;
	}
	public ResponseParamDTO<Map<String,Object>> getUnitProgress(String traceId, String classId, String courseId, String unitId, String userUid, boolean isSecure) throws Exception {
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
			String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
			}
			OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
			lessonDataAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
			lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);
			if (!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
				ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
				lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());

				//fetch item progress data
				OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
				ColumnList<String> items = itemData.getResult();
				for (Column<String> item : items) {
					Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
					String itemGooruOid = item.getName();
					// fetch item usage data
					itemDataAsMap.putAll(getClassMetricsAsMap(traceId, classLessonKey, itemGooruOid));
					itemDataMapAsList.add(itemDataAsMap);

					//fetch lesson's score status data
					OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
							getBaseService().appendTilda(classLessonKey, ApiConstants.ASSESSMENT, ApiConstants.SCOREINPERCENTAGE));
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
	
	public ResponseParamDTO<Map<String,Object>> getLessonPlan(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentIds, String userUid, boolean isSecure) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> resultDataMapAsList = new ArrayList<Map<String, Object>>();
		String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonId);
		if (StringUtils.isNotBlank(userUid)) {
			classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
		}
		resultDataMapAsList = getClassMetricsForAllItemsAsMap(traceId, classLessonKey, assessmentIds);
		responseParamDTO.setContent(resultDataMapAsList);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String, Object>> getStudentAssessmentData(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentId, String userUid,
			boolean isSecure) throws Exception {
		// Fetch goal for the class
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
		Map<String, Object> itemDetailAsMap = new HashMap<String, Object>();
		OperationResult<ColumnList<String>> classData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
		Long classMinScore = 0L;
		if (!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
			classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
		}
		
		//Fetch score and evidence of assessment
		String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonId);
		if (StringUtils.isNotBlank(userUid)) {
			classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
		}
		Long scoreInPercentage = null;
		Long score = null;
		String evidence = null;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey);
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> lessonMetricColumns = itemsColumnList.getResult();
			score = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants.SCORE), null);
			scoreInPercentage = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants.SCORE_IN_PERCENTAGE), null);
			evidence = lessonMetricColumns.getStringValue(getBaseService().appendTilda(assessmentId, ApiConstants.EVIDENCE), null);
		}
		
		//Fetch assessment metadata
		List<Map<String, Object>> rawDataMapAsList = new ArrayList<Map<String, Object>>();
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCETYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, assessmentId, resourceColumns);
		if (!rawDataMapAsList.isEmpty() && rawDataMapAsList.size() > 0) {
			itemDetailAsMap.putAll(rawDataMapAsList.get(0));
		}
		//Fetch assessment count
		Long assessmentCount = null;
		Map<String, Long> contentMetaAsMap = getContentMeta(traceId, assessmentId, ApiConstants.ASSESSMENT_COUNT);
		if(!contentMetaAsMap.isEmpty()) {
			assessmentCount = contentMetaAsMap.get(ApiConstants.ASSESSMENT_COUNT);
		}
		
		//Fetch username and profile url
		String username = null;
		OperationResult<ColumnList<String>> userColumnList = getCassandraService().read(traceId, ColumnFamily.USER.getColumnFamily(), userUid);
		if (!userColumnList.getResult().isEmpty() && userColumnList.getResult().size() > 0) {
			username = userColumnList.getResult().getStringValue(ApiConstants.USERNAME, null);
		}
		
		//Get sessions 
		ResponseParamDTO<Map<String, Object>> sessionResponse = getUserSessions(traceId, classId, courseId, unitId, lessonId, assessmentId, ApiConstants.ASSESSMENT, userUid, false, isSecure);
		itemDetailAsMap.put(ApiConstants.GOORUOID, assessmentId);
		itemDetailAsMap.put(ApiConstants.USERNAME, username);
		itemDetailAsMap.put(ApiConstants.PROFILEURL, filePath.getProperty(ApiConstants.USER_PROFILE_URL_PATH).replaceAll(ApiConstants.ID, userUid));
		itemDetailAsMap.put(ApiConstants.GOAL, classMinScore);
		itemDetailAsMap.put(ApiConstants.SCOREINPERCENTAGE, scoreInPercentage);
		itemDetailAsMap.put(ApiConstants.EVIDENCE, evidence);
		itemDetailAsMap.put(ApiConstants.SCORE, score);
		itemDetailAsMap.put(ApiConstants.ASSESSMENT_COUNT, assessmentCount);
		itemDetailAsMap.put(ApiConstants.SESSION, sessionResponse.getContent());
		itemDataMapAsList.add(itemDetailAsMap);
		responseParamDTO.setContent(itemDataMapAsList);
		return responseParamDTO;
	}
	
	private Map<String, Long> getContentMeta(String traceId, String key, String fetchColumnList) {
		Map<String, Long> contentMetaAsMap = new HashMap<String, Long>();
		OperationResult<ColumnList<String>> contentMetaColumnList = getCassandraService().read(traceId, ColumnFamily.CONTENT_META.getColumnFamily(), key);
		if (!contentMetaColumnList.getResult().isEmpty() && contentMetaColumnList.getResult().size() > 0) {
			ColumnList<String> contentMetaColumns = contentMetaColumnList.getResult();
			for(String columnNameTofetch : fetchColumnList.split(ApiConstants.COMMA)) {
				contentMetaAsMap.put(columnNameTofetch, contentMetaColumns.getLongValue(columnNameTofetch, null));
			}
		}
		return contentMetaAsMap;
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
	
	private Map<String, Object> getResourceMetaData(Map<String, Object> dataMap, String traceId,String type, String key) {
		// fetch metadata
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCETYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		ColumnList<String> resourceColumn = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), key, resourceColumns).getResult();
		if(type != null){
			String resourceType = resourceColumn.getStringValue(ApiConstants.RESOURCETYPE, "");
			if(!resourceType.matches(type)){
					return dataMap;
				}
		}
		for (Column<String> column : resourceColumn) {
			if(column.getName().equals(ApiConstants.RESOURCETYPE)){
				dataMap.put(ApiConstants.TYPE, column.getStringValue());
			}else{
			dataMap.put(column.getName(), column.getStringValue());
			}
		}
		return dataMap;
	}

	private Map<String, Object> getSessionMetricsAsMap(String traceId, String sessionKey, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
		OperationResult<ColumnList<String>> sessionColumnResult = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), sessionKey);
		if (!sessionColumnResult.getResult().isEmpty() && sessionColumnResult.getResult().size() > 0) {
			String sessionId = sessionColumnResult.getResult().getColumnByName(ApiConstants.SESSIONID).getStringValue();
			OperationResult<ColumnList<String>> sessionMetricsColumnResult = getCassandraService().read(traceId, ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionId);
			if (!sessionMetricsColumnResult.getResult().isEmpty() && sessionMetricsColumnResult.getResult().size() > 0) {
				ColumnList<String> sessionMetricColumns = sessionMetricsColumnResult.getResult();
				Long views = sessionMetricColumns.getLongValue(getBaseService().appendTilda(sessionKey, ApiConstants.VIEWS), null);
				Long timeSpent = sessionMetricColumns.getLongValue(getBaseService().appendTilda(sessionKey, ApiConstants.TIME_SPENT), null);
				Long score = sessionMetricColumns.getLongValue(getBaseService().appendTilda(sessionKey, ApiConstants.SCORE_IN_PERCENTAGE), null);
				usageAsMap.put(ApiConstants.VIEWS, views);
				usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
				usageAsMap.put(ApiConstants.SCOREINPERCENTAGE, score);
				return usageAsMap;
			}
		}
		return usageAsMap;
	}
	
	private Map<String, Object> getClassMetricsAsMap(String traceId, String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			Long views = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants.VIEWS), null);
			Long timeSpent = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants.TIME_SPENT), null);
			Long score = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants.SCORE_IN_PERCENTAGE), null);
			String collectionType = itemMetricColumns.getStringValue(getBaseService().appendTilda(contentGooruOid, ApiConstants.COLLECTION_TYPE), null);
			usageAsMap.put(ApiConstants.VIEWS, views);
			usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
			usageAsMap.put(ApiConstants.SCOREINPERCENTAGE, score);
			usageAsMap.put(ApiConstants.COLLECTIONTYPE, collectionType);
			return usageAsMap;
		}
		return usageAsMap;
	}
	
	private List<Map<String, Object>> getClassMetricsForAllItemsAsMap(String traceId, String key, String contentGooruOids) {
		List<Map<String, Object>> usageAsMapAsList = null;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		usageAsMapAsList = new ArrayList<Map<String, Object>>();
		for (String itemGooruOid : contentGooruOids.split(ApiConstants.COMMA)) {
			Map<String, Object> usageAsMap = new HashMap<String, Object>();
			usageAsMap.put(ApiConstants.GOORUOID, itemGooruOid);
			if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
				ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
				Long views = itemMetricColumns.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants.VIEWS), null);
				Long timeSpent = itemMetricColumns.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants.TIME_SPENT), null);
				Long score = itemMetricColumns.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants.SCORE_IN_PERCENTAGE), null);
				String collectionType = itemMetricColumns.getStringValue(getBaseService().appendTilda(itemGooruOid, ApiConstants.COLLECTION_TYPE), null);
				Long lastAccessed = itemMetricColumns.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants.LAST_ACCESSED), null);
				String evidence = itemMetricColumns.getStringValue(getBaseService().appendTilda(itemGooruOid, ApiConstants.EVIDENCE), null);
				usageAsMap.put(ApiConstants.VIEWS, views);
				usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
				usageAsMap.put(ApiConstants.SCOREINPERCENTAGE, score);
				usageAsMap.put(ApiConstants.COLLECTIONTYPE, collectionType);
				usageAsMap.put(ApiConstants.LASTACCESSED, lastAccessed);
				usageAsMap.put(ApiConstants.EVIDENCE, evidence);
				usageAsMapAsList.add(usageAsMap);
			}
		}
		return usageAsMapAsList;
	}
	
	private Map<String, Object> getActivityMetricsAsMap(String traceId, String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			Long views = itemMetricColumns.getLongValue(ApiConstants.VIEWS, null);
			Long timeSpent = itemMetricColumns.getLongValue(ApiConstants.TIME_SPENT, null);
			String collectionType = itemMetricColumns.getStringValue(ApiConstants.COLLECTION_TYPE, null);
			usageAsMap.put(ApiConstants.VIEWS, views);
			usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
			usageAsMap.put(ApiConstants.COLLECTIONTYPE, collectionType);
			return usageAsMap;
		}
		return usageAsMap;
	}
	
	private List<Map<String,Object>> getCollectionActivityMetrics(String traceId, Collection<String> rowKeys, Collection<String> columns, String userIds) {

		List<Map<String,Object>> collectionUsageData = new ArrayList<Map<String,Object>>();
		OperationResult<Rows<String, String>> activityData = getCassandraService().readAll(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), rowKeys, columns);
		if (!activityData.getResult().isEmpty()) {
			Rows<String, String> itemMetricRows = activityData.getResult();
			for(Row<String, String> metricRow : itemMetricRows){
				String userId = null;
				Map<String,Map<String, Object>> KeyUsageAsMap = new HashMap<String,Map<String, Object>>();
				for(String column : columns){
					Map<String,Object> usageMap = new HashMap<String,Object>();
					String[] columnMetaInfo = column.split(ApiConstants.TILDA);
					String metricName = (columnMetaInfo.length > 1) ? columnMetaInfo[columnMetaInfo.length-1] : columnMetaInfo[0];
					if(metricName.equalsIgnoreCase(ApiConstants.COLLECTION_TYPE)){
						usageMap.put(ApiConstants.COLLECTIONTYPE, metricRow.getColumns().getStringValue(column, null));
					}else if(metricName.equalsIgnoreCase(ApiConstants.VIEWS)){
						usageMap.put(ApiConstants.VIEWS, metricRow.getColumns().getLongValue(column.trim(), 0L));
					}else if(metricName.equalsIgnoreCase(ApiConstants.SCORE_IN_PERCENTAGE)){
						usageMap.put(ApiConstants.SCOREINPERCENTAGE, metricRow.getColumns().getLongValue(column.trim(), 0L));
					}else if(metricName.equalsIgnoreCase(ApiConstants.TIME_SPENT)){
						usageMap.put(ApiConstants.TIMESPENT, metricRow.getColumns().getLongValue(column.trim(), 0L));
					}else {
						try{
							usageMap.put(metricName, metricRow.getColumns().getLongValue(column, 0L));
						}catch(Exception e){
							InsightsLogger.error(traceId, getBaseService().errorHandler(ErrorMessages.UNHANDLED_EXCEPTION, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), column),e);
						}
					}
					if(userIds != null){
						for(String id : userIds.split(ApiConstants.COMMA)){
							if(metricRow.getKey().contains(id)){
								userId = id;
								break;
							}
					}
					usageMap.put(ApiConstants.USERUID, userId);
					}
					if(KeyUsageAsMap.containsKey(columnMetaInfo[0])){
						usageMap.putAll(KeyUsageAsMap.get(columnMetaInfo[0]));
					}
						KeyUsageAsMap.put(columnMetaInfo[0], usageMap);
				}
				collectionUsageData.addAll(getBaseService().convertMapToList(KeyUsageAsMap, ApiConstants.GOORUOID));
			}
		}
		return collectionUsageData.isEmpty() ? null : collectionUsageData;
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
					} else if (contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT) || contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT_URL)) {
						if (assessmentGooruOids.length() > 0) {
							assessmentGooruOids.append(COMMA);
						}
						assessmentGooruOids.append(itemGooruOid);
					}
				}
				if (itemGooruOids.length() > 0) {
					itemGooruOids.append(COMMA);
				}
				itemGooruOids.append(itemGooruOid);
			}
		}
	}

	private List<Map<String,Object>> getStudents(String traceId, String classId){
		
		OperationResult<ColumnList<String>> usersData = getCassandraService().read(traceId, ColumnFamily.USER_GROUP_ASSOCIATION.getColumnFamily(), classId);
		List<Map<String,Object>> userList = new ArrayList<Map<String,Object>>();
		for(Column<String> column : usersData.getResult()){
			Map<String,Object> userMap = new HashMap<String,Object>();
			userMap.put(ApiConstants.USERUID, column.getName());
			userMap.put(ApiConstants.USERNAME, column.getStringValue());
			userList.add(userMap);
		}
		return userList;
	} 
	
	private List<Map<String,Object>> getContentItems(String traceId,String rowKey,String type,boolean fetchMetaData){
		
		OperationResult<ColumnList<String>> assessmentData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), rowKey);
		List<Map<String,Object>> contentItems = new ArrayList<Map<String,Object>>();
		for(Column<String> column : assessmentData.getResult()){
			Map<String, Object> dataMap = new HashMap<String,Object>();
			if(fetchMetaData){
				getResourceMetaData(dataMap, traceId,type, column.getName());
				if(dataMap.isEmpty()){
					continue;
				}
			}
			dataMap.put(ApiConstants.SEQUENCE, column.getIntegerValue());
			dataMap.put(ApiConstants.GOORUOID, column.getName());
			contentItems.add(dataMap);
		}
		return contentItems;
	}
	private ResponseParamDTO<Map<String,Object>> getAllStudentProgressByUnit(String traceId, String classId, String courseId,boolean isSecure) throws Exception {
		
		List<Map<String, Object>> unitDetails = new ArrayList<Map<String, Object>>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		
		OperationResult<ColumnList<String>> unitData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), courseId);
		ColumnList<String> units = unitData.getResult();
		
		OperationResult<ColumnList<String>> userGroupAssociation = getCassandraService().read(traceId, ColumnFamily.USER_GROUP_ASSOCIATION.getColumnFamily(), classId);
		ColumnList<String> userGroup = userGroupAssociation.getResult();
		
		if(!units.isEmpty() && units.size() > 0) {
			for (String unitGooruOid : units.getColumnNames()) {
				Map<String, Object> unitDataAsMap = new HashMap<String, Object>();
				/**
				 * Get Resource Meta data
				 */
				Collection<String> resourceColumns = new ArrayList<String>();
				resourceColumns.add(ApiConstants.TITLE);
				resourceColumns.add(ApiConstants.RESOURCETYPE);
				OperationResult<ColumnList<String>> unitMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), unitGooruOid, resourceColumns);
				if (!unitMetaData.getResult().isEmpty() && unitMetaData.getResult().size() > 0) {
					ColumnList<String> unitMetaDataColumns = unitMetaData.getResult();
					unitDataAsMap.put(ApiConstants.GOORUOID, unitGooruOid);
					unitDataAsMap.put(ApiConstants.TITLE, unitMetaDataColumns.getStringValue(ApiConstants.TITLE,null));
					unitDataAsMap.put(ApiConstants.RESOURCETYPE, unitMetaDataColumns.getStringValue(ApiConstants.RESOURCETYPE, null));
					unitDataAsMap.put(SEQUENCE, units.getLongValue(unitGooruOid, 0L));
						List<Map<String, Object>> unitUsageDetails = new ArrayList<Map<String, Object>>();
						for (Column<String> user : userGroup) {
							/**
							 * Get activity details
							 */
							Map<String, Object> unitUsageData = new HashMap<String, Object>();
							unitUsageData.put(ApiConstants.USERUID, user.getName());
							unitUsageData.put(ApiConstants.USERNAME, user.getStringValue());

							Long scoreInPercentage = 0L;
							OperationResult<ColumnList<String>> unitActivityDetails = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
									baseService.appendTilda(classId, courseId, unitGooruOid,user.getName()));
							if(unitActivityDetails != null){
								ColumnList<String> unitActivity = unitActivityDetails.getResult();
								if(!unitActivity.isEmpty()){
									scoreInPercentage = unitActivity.getLongValue(ApiConstants.SCORE_IN_PERCENTAGE, 0L);
								}
							}
							unitUsageData.put(ApiConstants.SCOREINPERCENTAGE, scoreInPercentage);
							unitUsageDetails.add(unitUsageData);
						}
						unitDataAsMap.put(ApiConstants.USAGEDATA, unitUsageDetails);
				}
				unitDetails.add(unitDataAsMap);
			}
		}
		responseParamDTO.setContent(unitDetails);
		return responseParamDTO;
	}
}