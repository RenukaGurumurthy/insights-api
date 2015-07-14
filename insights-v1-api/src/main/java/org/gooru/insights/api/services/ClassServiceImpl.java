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
import org.gooru.insights.api.constants.ApiConstants.SessionAttributes;
import org.gooru.insights.api.constants.ApiConstants.options;
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.utils.DataUtils;
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
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, unitGooruOids.toString(), resourceColumns, ApiConstants.UNIT);
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
				usageAsMap.put(ApiConstants.USAGE_DATA, usageDataAsMap);
				resultMapList.add(usageAsMap);
			}
			responseParamDTO.setContent(resultMapList);
		}
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> getUnitUsage(String traceId, String classId, String courseId, String unitId, String userUid, String collectionType, Boolean getUsageData, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String,Object>> students = null;
		List<Map<String,Object>> resultData = new ArrayList<Map<String, Object>>();
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
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		unitRawDataMapAsList = getResourceData(traceId, isSecure, unitRawDataMapAsList, unitId, resourceColumns, ApiConstants.UNIT);

		
		//fetch metadata of lessons
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, lessonGooruOids.toString(), resourceColumns, ApiConstants.LESSON);
		responseParamDTO.setContent(rawDataMapAsList);

		//fetch usage data of unit
		if(getUsageData) {
			String unitKey = getBaseService().appendTilda(classId, courseId, unitId);
			
			String classUnitKey = unitKey;
			//fetch unit's item views/attempts count
			if(StringUtils.isNotBlank(userUid)) {
				classUnitKey += getBaseService().appendTilda(classUnitKey, userUid);
			}
			OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), getBaseService().appendTilda(classUnitKey, ApiConstants.COLLECTION, ApiConstants._TIME_SPENT));
			long collectionsViewedInUnit = collectionMetricsData != null ? collectionMetricsData.getResult().size() : 0L;
			OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), getBaseService().appendTilda(classUnitKey, ApiConstants.ASSESSMENT, ApiConstants._SCORE_IN_PERCENTAGE));
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
				if (lessons != null) {
						usageAsMap.put(ApiConstants.SEQUENCE, lessons.getColumnByName(lessonGooruOid) != null ? lessons.getLongValue(lessonGooruOid, 0L) : 0L);
				}
				usageAsMap.put(ApiConstants.ASSESSMENT_COUNT, assessments.size());
				usageAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
				

				//fetch lesson usage data
				Map<String, Object> lessonUsageAsMap = new HashMap<String, Object>();
				String classLessonKey = getBaseService().appendTilda(unitKey, lessonGooruOid);
				if (StringUtils.isNotBlank(userUid)) {
					classLessonKey += getBaseService().appendTilda(classLessonKey, userUid);
					lessonUsageAsMap.putAll(getActivityMetricsAsMap(traceId, classLessonKey, lessonGooruOid));
					
					usageAsMap.put(ApiConstants.USAGE_DATA, lessonUsageAsMap);

					usageAsMap.putAll(rawDataMap);
					lessonResultMapList.add(usageAsMap);
				}else {
						/**
						 * Fetch the list of user usage data and store it in lessonUsageAsMap as userUsage
						 */
					String contentType = null;
					if(collectionType !=null){
						if(collectionType.equalsIgnoreCase(ApiConstants.ASSESSMENT)){
							contentType = ApiConstants.ASSESMENT_TYPE_MATCHER;
						}else if(collectionType.matches(ApiConstants.COLLECTION_MATCH)) {
							contentType = ApiConstants.COLLECTION_MATCH;
						}
					}
					List<Map<String,Object>> collections = getContentItems(traceId,lessonGooruOid,contentType,true);
					students = getStudents(traceId,classId);
					if(!(students.isEmpty() || collections.isEmpty())){
						Set<String> columnSuffix = new HashSet<String>();
						columnSuffix.add(ApiConstants._TIME_SPENT);
						columnSuffix.add(ApiConstants._SCORE_IN_PERCENTAGE);
						StringBuffer studentIds = getBaseService().exportData(students, ApiConstants.USERUID);
						StringBuffer collectionIds = getBaseService().exportData(collections, ApiConstants.GOORUOID);
						Collection<String> rowKeys = getBaseService().appendAdditionalField(ApiConstants.TILDA,classLessonKey, studentIds.toString());
						Collection<String> columns = getBaseService().appendAdditionalField(ApiConstants.TILDA,collectionIds.toString(), columnSuffix);
						/**
						 * Get collection activity
						 */
						List<Map<String,Object>> assessmentUsage = getCollectionActivityMetrics(traceId, rowKeys,ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), columns, studentIds.toString(),true,collectionIds.toString(),true);
						/**
						 * Existing JSON Structure, will be removed while the API got stabilized
						 */
/*						assessmentUsage = getBaseService().includeDefaultData(assessmentUsage, students, ApiConstants.GOORUOID, ApiConstants.USERUID);
						System.out.println("assessmentUsage:"+assessmentUsage);
						assessmentUsage = getBaseService().groupDataDependOnkey(assessmentUsage,ApiConstants.GOORUOID,ApiConstants.USAGE_DATA);
						System.out.println("groupDataDependOnkey:"+assessmentUsage);
						assessmentUsage = getBaseService().LeftJoin(collections,assessmentUsage,ApiConstants.GOORUOID,ApiConstants.GOORUOID);
*/						/**
						 * Newer JSON Structure
						 */
						assessmentUsage = getBaseService().includeDefaultData(assessmentUsage, collections, ApiConstants.USERUID, ApiConstants.GOORUOID);
						assessmentUsage = getBaseService().groupDataDependOnkey(assessmentUsage,ApiConstants.USERUID,ApiConstants.USAGE_DATA);
						usageAsMap.putAll(rawDataMap);
						assessmentUsage = getBaseService().injectRecord(assessmentUsage, usageAsMap);
						resultData.addAll(assessmentUsage);
					}
				}
			}
			if (StringUtils.isNotBlank(userUid)) {
			unitUsageDataAsMap.put(ApiConstants.LESSON, lessonResultMapList);
				if(!unitRawDataMapAsList.isEmpty() && unitRawDataMapAsList.size() > 0) {
					unitUsageDataAsMap.putAll(unitRawDataMapAsList.get(0));
					if(unitUsageDataAsMap.containsKey(ApiConstants.RESOURCE_TYPE) && unitUsageDataAsMap.get(ApiConstants.RESOURCE_TYPE) != null) {
						unitUsageDataAsMap.put(ApiConstants.TYPE , unitUsageDataAsMap.get(ApiConstants.RESOURCE_TYPE));
						unitUsageDataAsMap.remove(ApiConstants.RESOURCE_TYPE);
					}
				}
				resultMapList.add(unitUsageDataAsMap);
				responseParamDTO.setContent(resultMapList);
			}else{
				resultData = getBaseService().groupDataDependOnkey(resultData,ApiConstants.USERUID,ApiConstants.USAGE_DATA);
				resultData = getBaseService().LeftJoin(resultData,students,ApiConstants.USERUID,ApiConstants.USERUID);
				if(!resultData.isEmpty()){
					responseParamDTO.setContent(resultData);
				}else{
					responseParamDTO.setContent(null);
				}
			}
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
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, itemGooruOids.toString(), resourceColumns, ApiConstants.RESOURCE);
		responseParamDTO.setContent(rawDataMapAsList);

		// fetch usage data
		if (getUsageData) {
			OperationResult<ColumnList<String>>  classData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
			Long classMinScore = 0L;
			if(!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
				classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
			}
			String lessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonId);
			
			//fetch unit's item views/attempts count
			String classLessonKey = lessonKey;
			if(StringUtils.isNotBlank(userUid)) {
				classLessonKey += getBaseService().appendTilda(classLessonKey, userUid);
			}
			OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), getBaseService().appendTilda(classLessonKey, ApiConstants.COLLECTION, ApiConstants._TIME_SPENT));
			long collectionsViewedInLesson = collectionMetricsData != null ? collectionMetricsData.getResult().size() : 0L;
			OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), getBaseService().appendTilda(classLessonKey, ApiConstants.ASSESSMENT, ApiConstants._SCORE_IN_PERCENTAGE));
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
				usageDataAsMap.put(ApiConstants.COLLECTION_COUNT, collectionCount);

				// fetch lesson usage data
				Map<String, Object> lessonUsageAsMap = new HashMap<String, Object>();
				lessonUsageAsMap = getActivityMetricsAsMap(traceId, classLessonKey, lessonId);
				if (!lessonUsageAsMap.isEmpty()) {
					usageDataAsMap.put(ApiConstants.LESSON_USAGE_DATA, lessonUsageAsMap);
				}

				// fetch collection usage data
				String classCollectionKey = getBaseService().appendTilda(lessonKey, itemGooruOid);
				if (StringUtils.isNotBlank(userUid)) {
					classCollectionKey += getBaseService().appendTilda(classCollectionKey, userUid);
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
				usageAsMap.put(ApiConstants.USAGE_DATA, usageDataAsMap);
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
			unitDataAsMap.put(ApiConstants.GOORUOID, unitGooruOid);
			unitDataAsMap.put(ApiConstants.TYPE, ApiConstants.UNIT);
			try {
				unitDataAsMap.put(ApiConstants.SEQUENCE, unit.getLongValue());
			} catch (Exception e) {
				unitDataAsMap.put(ApiConstants.SEQUENCE, unit.getIntegerValue());
			}
			OperationResult<ColumnList<String>> unitMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), unitGooruOid, resourceColumns);
			if (!unitMetaData.getResult().isEmpty() && unitMetaData.getResult().size() > 0) {
				ColumnList<String> unitMetaDataColumns = unitMetaData.getResult();
				unitDataAsMap.put(ApiConstants.TITLE, unitMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
			}
			
			OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitGooruOid);
			ColumnList<String> lessons = lessonData.getResult();
			for (Column<String> lesson : lessons) {
				long scoreMet = 0;
				long scoreNotMet = 0;
				long attempted = 0;
				Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
				String lessonGooruOid = lesson.getName();
				String classLessonKey = getBaseService().appendTilda(classId, courseId, unitGooruOid, lessonGooruOid);
				if (StringUtils.isNotBlank(userUid)) {
					classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
				}
				OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
				lessonDataAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
				lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);
				try {
					lessonDataAsMap.put(ApiConstants.SEQUENCE, lesson.getLongValue());
				} catch (Exception e) {
					lessonDataAsMap.put(ApiConstants.SEQUENCE, lesson.getIntegerValue());
				}
				if (!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
					ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
					lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
				}
				OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
				ColumnList<String> items = itemData.getResult();
				for (Column<String> item : items) {
					String itemGooruOid = item.getName();
					OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
							getBaseService().appendTilda(classLessonKey, ApiConstants.ASSESSMENT, ApiConstants._SCORE_IN_PERCENTAGE));
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
				if (attempted == 0) {
					lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.NOT_ATTEMPTED);
				} else if (scoreMet > 0 && scoreNotMet == 0 && (scoreMet == items.size())) {
					lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_MET);
				}
				lessonDataMapAsList.add(lessonDataAsMap);
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
		OperationResult<ColumnList<String>> classData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
		Long classMinScore = 0L;
		if (!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
			classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
		}
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.GOORUOID);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);

		
		OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitId);
		ColumnList<String> lessons = lessonData.getResult();
		for (Column<String> lesson : lessons) {
			long scoreNotMet = 0;
			long scoreMet = 0;
			long attempted = 0;
			long notAttempted = 0;
			List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
			Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
			String lessonGooruOid = lesson.getName();
			String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
			}
			lessonDataAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
			lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);
			try {
				lessonDataAsMap.put(ApiConstants.SEQUENCE, lesson.getLongValue());
			} catch (Exception e) {
				lessonDataAsMap.put(ApiConstants.SEQUENCE, lesson.getIntegerValue());
			}
			OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
			if (!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
				ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
				lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
			}
			OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
			ColumnList<String> items = itemData.getResult();
			for (Column<String> item : items) {
				String itemGooruOid = item.getName();
				Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
				try {
					itemDataAsMap.put(ApiConstants.SEQUENCE, item.getLongValue());
				} catch (Exception e) {
					itemDataAsMap.put(ApiConstants.SEQUENCE, item.getIntegerValue());
				}

				List<Map<String, Object>> rawDataMapAsList = new ArrayList<Map<String, Object>>();
				rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, itemGooruOid, resourceColumns, ApiConstants.COLLECTION);
				if (rawDataMapAsList.size() > 0) {
					itemDataAsMap.putAll(rawDataMapAsList.get(0));
					if(itemDataAsMap.containsKey(ApiConstants.RESOURCE_TYPE) && itemDataAsMap.get(ApiConstants.RESOURCE_TYPE) != null) {
						itemDataAsMap.put(ApiConstants.TYPE , itemDataAsMap.get(ApiConstants.RESOURCE_TYPE));
						itemDataAsMap.remove(ApiConstants.RESOURCE_TYPE);
					}
				}

				OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
						getBaseService().appendTilda(classLessonKey, ApiConstants.ASSESSMENT, ApiConstants._SCORE_IN_PERCENTAGE));
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
					}
				}
				if (assessmentScore == null) {
					scoreNotMet += 1; 
					notAttempted += 1;
					assessmentScoreStatus = ApiConstants.NOT_ATTEMPTED;
				}
				itemDataAsMap.put(ApiConstants.SCORE_STATUS, assessmentScoreStatus);
				itemDataMapAsList.add(itemDataAsMap);
			}
			lessonDataAsMap.put(ApiConstants.ITEM, itemDataMapAsList);
			if (attempted == 0 && (itemDataMapAsList.size() == notAttempted)) {
				lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.NOT_ATTEMPTED);
			} else if(scoreNotMet > 1) { 
				lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_NOT_MET);
			} else if (scoreMet > 0 && scoreNotMet == 0 && (itemDataMapAsList.size() == scoreMet)) {
				lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_MET);
			}
			lessonDataMapAsList.add(lessonDataAsMap);
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
				for (Column<String> unit : units) {
					Map<String, Object> unitDataAsMap = new HashMap<String, Object>();
					String unitGooruOid = unit.getName();
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
					try{
						unitDataAsMap.put(ApiConstants.SEQUENCE, unit.getLongValue());
					}catch (Exception e) {
						unitDataAsMap.put(ApiConstants.SEQUENCE, unit.getIntegerValue());
					}
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
							getBaseService().appendTilda(classUnitKey, ApiConstants.COLLECTION, ApiConstants._TIME_SPENT));
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
							getBaseService().appendTilda(classUnitKey, ApiConstants.ASSESSMENT, ApiConstants._SCORE_IN_PERCENTAGE));
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
					unitDataAsMap.put(ApiConstants.TOTAL_STUDY_TIME, totalStudyTime);
					unitDataAsMap.put(ApiConstants.ASSESSMENTS_ATTEMPTED, assessmentAttempted);
					unitDataAsMap.put(ApiConstants.AVG_SCORE, avgScore);
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
		String key = null;
		if (StringUtils.isNotBlank(collectionId) && StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(classId) && StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(unitId)
				&& StringUtils.isNotBlank(lessonId)) {
			key = baseService.appendTilda(classId, courseId, unitId, lessonId, collectionId, userUid);
		} else if (StringUtils.isNotBlank(collectionId) && StringUtils.isNotBlank(userUid)) {
			key = baseService.appendTilda(collectionId, userUid);
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E106);
		}
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		OperationResult<ColumnList<String>> sessions = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), key);
		List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
		if (sessions != null) {
			ColumnList<String> sessionList = sessions.getResult();
			if (!sessionList.isEmpty()) {
				int sequence = 1;
				if (!fetchOpenSession) {
					for (Column<String> sessionColumn : sessionList) {
						resultSet.add(generateSessionMap(sequence++, sessionColumn.getName(), sessionColumn.getLongValue()));
					}
				} else {
					ColumnList<String> sessionsInfo = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), baseService.appendTilda(key, INFO)).getResult();
					for (Column<String> sessionColumn : sessionList) {
						if (sessionsInfo.getStringValue(baseService.appendTilda(sessionColumn.getName(), TYPE), null).equalsIgnoreCase(START)) {
							Map<String, Object> sessionInfoMap = generateSessionMap(sequence++, sessionColumn.getName(), sessionColumn.getLongValue());
							sessionInfoMap.put(LAST_ACCESSED_RESOURCE, sessionsInfo.getStringValue(baseService.appendTilda(sessionColumn.getName(), _LAST_ACCESSED_RESOURCE), null));
							resultSet.add(sessionInfoMap);
						}
					}
				}
				resultSet = baseService.sortBy(resultSet, EVENT_TIME, ApiConstants.ASC);
			}
		}
		responseParamDTO.setContent(resultSet);
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
			try {
				lessonDataAsMap.put(ApiConstants.SEQUENCE, lesson.getLongValue());
			} catch (Exception e) {
				lessonDataAsMap.put(ApiConstants.SEQUENCE, lesson.getIntegerValue());
			}
			String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
			}
			OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
			lessonDataAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
			lessonDataAsMap.put(ApiConstants.TITLE, null);
			if (!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
				ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
				lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
			}
			// fetch item progress data
			OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
			ColumnList<String> items = itemData.getResult();
			for (Column<String> item : items) {
				Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
				String itemGooruOid = item.getName();
				// fetch item usage data
				itemDataAsMap.putAll(getClassMetricsAsMap(traceId, classLessonKey, itemGooruOid));
				itemDataMapAsList.add(itemDataAsMap);

				// fetch lesson's score status data
				OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
						getBaseService().appendTilda(classLessonKey, ApiConstants.ASSESSMENT, ApiConstants._SCORE_IN_PERCENTAGE));
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
			lessonDataAsMap.putAll(getActivityMetricsAsMap(traceId, classLessonKey, lessonGooruOid));
			lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);
			lessonDataAsMap.put(ApiConstants.ITEM, itemDataMapAsList);
			if (attempted == 0) {
				lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.NOT_ATTEMPTED);
			} else if (scoreMet > 0 && scoreNotMet == 0) {
				lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_MET);
			}

			if (!lessonDataAsMap.isEmpty() && lessonDataAsMap.size() > 0) {
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
	
	public ResponseParamDTO<Map<String, Object>> getStudentAssessmentData(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentId, String sessionId, String userUid,
			String collectionType, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
		Map<String, Object> itemDetailAsMap = new HashMap<String, Object>();
		OperationResult<ColumnList<String>> itemsColumnList = null;
		Long classMinScore = 0L; String classLessonKey = null;
		Long scoreInPercentage = 0L; Long score = 0L; String evidence = null; Long timespent = 0L;
		
		if ((classId != null && StringUtils.isNotBlank(classId.trim())) && (courseId != null && StringUtils.isNotBlank(courseId.trim())) 
				&& (unitId != null && StringUtils.isNotBlank(unitId.trim())) && (lessonId != null && StringUtils.isNotBlank(lessonId.trim()))) {
			// Fetch goal for the class
			OperationResult<ColumnList<String>> classData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
			if (!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
				classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
			}
			
			classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonId);
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
			}
			itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey);
		} else if ((sessionId != null && StringUtils.isNotBlank(sessionId.trim()))) {
			itemsColumnList = getCassandraService().read(traceId, ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionId);
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E111, getBaseService().appendComma("contentGooruId", "sessionId"), getBaseService().appendComma("classId","courseId","unitId","lessonId","contentGooruId"));
		}
		
		//Fetch score and evidence of assessment
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> lessonMetricColumns = itemsColumnList.getResult();
			score = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants.SCORE), 0L);
			scoreInPercentage = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._SCORE_IN_PERCENTAGE), 0L);
			evidence = lessonMetricColumns.getStringValue(getBaseService().appendTilda(assessmentId, ApiConstants.EVIDENCE), null);
			timespent = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants.TIMESPENT), 0L);
		}
		
		//Fetch assessment metadata
		List<Map<String, Object>> rawDataMapAsList = new ArrayList<Map<String, Object>>();
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, assessmentId, resourceColumns, ApiConstants.COLLECTION);
		if (!rawDataMapAsList.isEmpty() && rawDataMapAsList.size() > 0) {
			itemDetailAsMap.putAll(rawDataMapAsList.get(0));
			if(itemDetailAsMap.containsKey(ApiConstants.RESOURCE_TYPE) && itemDetailAsMap.get(ApiConstants.RESOURCE_TYPE) != null) {
				itemDetailAsMap.put(ApiConstants.TYPE , itemDetailAsMap.get(ApiConstants.RESOURCE_TYPE));
				itemDetailAsMap.remove(ApiConstants.RESOURCE_TYPE);
			}
		}
		
		// Fetch assessment count
		Long questionCount = 0L; Long scorableQuestionCount = 0L; Long oeCount = 0L; Long resourceCount = 0L; Long itemCount = 0L; 
		Map<String, Long> contentMetaAsMap = getContentMeta(traceId, assessmentId, getBaseService().appendComma(ApiConstants.QUESTION_COUNT, ApiConstants._OE_COUNT, ApiConstants.RESOURCE_COUNT, ApiConstants._ITEM_COUNT));
		if (!contentMetaAsMap.isEmpty()) {
			questionCount = (contentMetaAsMap.containsKey(ApiConstants.QUESTION_COUNT) && contentMetaAsMap.get(ApiConstants.QUESTION_COUNT) != null) ? contentMetaAsMap.get(ApiConstants.QUESTION_COUNT) : 0L;
			oeCount = (contentMetaAsMap.containsKey(ApiConstants._OE_COUNT) && contentMetaAsMap.get(ApiConstants._OE_COUNT) != null) ? contentMetaAsMap.get(ApiConstants._OE_COUNT) : 0L;
			resourceCount = (contentMetaAsMap.containsKey(ApiConstants.RESOURCE_COUNT) && contentMetaAsMap.get(ApiConstants.RESOURCE_COUNT) != null) ? contentMetaAsMap.get(ApiConstants.RESOURCE_COUNT) : 0L;
			itemCount = (contentMetaAsMap.containsKey(ApiConstants._ITEM_COUNT) && contentMetaAsMap.get(ApiConstants._ITEM_COUNT) != null) ? contentMetaAsMap.get(ApiConstants._ITEM_COUNT) : 0L;
			if (questionCount > 0) {
				scorableQuestionCount = questionCount - oeCount;
			}
		}
		itemDetailAsMap.put(ApiConstants.QUESTION_COUNT, questionCount);
		itemDetailAsMap.put(ApiConstants.OE_COUNT, oeCount);
		itemDetailAsMap.put(ApiConstants.SCORABLE_QUESTION_COUNT, scorableQuestionCount);
		itemDetailAsMap.put(ApiConstants.RESOURCE_COUNT, resourceCount);
		itemDetailAsMap.put(ApiConstants.ITEM_COUNT, itemCount);
		
		//Fetch username and profile url
		String username = null;
		OperationResult<ColumnList<String>> userColumnList = getCassandraService().read(traceId, ColumnFamily.USER.getColumnFamily(), userUid);
		if (!userColumnList.getResult().isEmpty() && userColumnList.getResult().size() > 0) {
			username = userColumnList.getResult().getStringValue(ApiConstants.USER_NAME, null);
		}
		
		//Get sessions 
		ResponseParamDTO<Map<String, Object>> sessionResponse = getUserSessions(traceId, classId, courseId, unitId, lessonId, assessmentId, ApiConstants.ASSESSMENT, userUid, false, isSecure);
		
		itemDetailAsMap.put(ApiConstants.GOORUOID, assessmentId);
		itemDetailAsMap.put(ApiConstants.USERNAME, username);
		itemDetailAsMap.put(ApiConstants.PROFILE_URL, filePath.getProperty(ApiConstants.USER_PROFILE_URL_PATH).replaceAll(ApiConstants.ID, userUid));
		itemDetailAsMap.put(ApiConstants.GOAL, classMinScore);
		itemDetailAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, scoreInPercentage);
		itemDetailAsMap.put(ApiConstants.EVIDENCE, evidence);
		itemDetailAsMap.put(ApiConstants.SCORE, score);
		itemDetailAsMap.put(ApiConstants.TIMESPENT, timespent);
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
	
	private List<Map<String, Object>> getResourceData(String traceId, boolean isSecure, List<Map<String, Object>> rawDataMapAsList, String keys, Collection<String> columnsToFetch, String type) {
		rawDataMapAsList = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.RESOURCE.getColumnFamily(), null, keys, new String(), columnsToFetch));
		Map<String, Map<Integer, String>> combineMap = new HashMap<String, Map<Integer, String>>();
		Map<Integer, String> filterMap = new HashMap<Integer, String>();
		filterMap.put(0, filePath.getProperty(ApiConstants.NFS_BUCKET));
		if(type.equalsIgnoreCase(ApiConstants.RESOURCE)) {
			filterMap.put(1, ApiConstants.FOLDER);
			filterMap.put(2, ApiConstants.THUMBNAIL);
		} else {
			filterMap.put(1, ApiConstants.THUMBNAIL);
		}
		combineMap.put(ApiConstants.THUMBNAIL, filterMap);
		rawDataMapAsList = getBaseService().appendInnerData(rawDataMapAsList, combineMap, isSecure ? ApiConstants.HTTPS : ApiConstants.HTTP);
		rawDataMapAsList = getBaseService().addCustomKeyInMapList(rawDataMapAsList, ApiConstants.GOORUOID, null);
		return rawDataMapAsList;
	}
	
	private void getResourceMetaData(Map<String, Object> dataMap, String traceId,String type, String key) {
		// fetch metadata
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		ColumnList<String> resourceColumn = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), key, resourceColumns).getResult();
		if(type != null){
			String resourceType = resourceColumn.getStringValue(ApiConstants.RESOURCE_TYPE, ApiConstants.STRING_EMPTY);
			if(!resourceType.matches(type)){
					return;
				}
		}
		for (Column<String> column : resourceColumn) {
			if(column.getName().equals(ApiConstants.RESOURCE_TYPE)){
				dataMap.put(ApiConstants.TYPE, column.getStringValue());
			}else{
			dataMap.put(column.getName(), column.getStringValue());
			}
		}
	}
	
	private Map<String, Object> getClassMetricsAsMap(String traceId, String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		Long views = 0L; Long timeSpent = 0L; Long score = 0L; String collectionType = null;
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			views = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants.VIEWS), 0L);
			timeSpent = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants._TIME_SPENT), 0L);
			score = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants._SCORE_IN_PERCENTAGE), 0L);
			collectionType = itemMetricColumns.getStringValue(getBaseService().appendTilda(contentGooruOid, ApiConstants._COLLECTION_TYPE), null);
		}
		usageAsMap.put(ApiConstants.VIEWS, views);
		usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
		usageAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, score);
		usageAsMap.put(ApiConstants.TYPE, collectionType);
		return usageAsMap;
	}
	
	private List<Map<String, Object>> getClassMetricsForAllItemsAsMap(String traceId, String key, String contentGooruOids) {
		List<Map<String, Object>> usageAsMapAsList = null;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		usageAsMapAsList = new ArrayList<Map<String, Object>>();
		for (String itemGooruOid : contentGooruOids.split(ApiConstants.COMMA)) {
			Map<String, Object> usageAsMap = new HashMap<String, Object>();
			usageAsMap.put(ApiConstants.GOORUOID, itemGooruOid);
			Long views = 0L; Long timeSpent = 0L; Long score = 0L; String collectionType = null;Long lastAccessed = null;String evidence = null;
			if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
				ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
				views = itemMetricColumns.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants.VIEWS), 0L);
				timeSpent = itemMetricColumns.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants._TIME_SPENT), 0L);
				score = itemMetricColumns.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants._SCORE_IN_PERCENTAGE), 0L);
				collectionType = itemMetricColumns.getStringValue(getBaseService().appendTilda(itemGooruOid, ApiConstants._COLLECTION_TYPE), null);
				lastAccessed = itemMetricColumns.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants._LAST_ACCESSED), null);
				evidence = itemMetricColumns.getStringValue(getBaseService().appendTilda(itemGooruOid, ApiConstants.EVIDENCE), null);
			}
			usageAsMap.put(ApiConstants.VIEWS, views);
			usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
			usageAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, score);
			usageAsMap.put(ApiConstants.TYPE, collectionType);
			usageAsMap.put(ApiConstants.LAST_ACCESSED, lastAccessed);
			usageAsMap.put(ApiConstants.EVIDENCE, evidence);
			usageAsMapAsList.add(usageAsMap);
		}
		return usageAsMapAsList;
	}
	
	private Map<String, Object> getActivityMetricsAsMap(String traceId, String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
		Long views = 0L; Long timeSpent = 0L; String collectionType = null; Long score = 0L;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			views = itemMetricColumns.getLongValue(ApiConstants.VIEWS, 0L);
			timeSpent = itemMetricColumns.getLongValue(ApiConstants._TIME_SPENT, 0L);
			score = itemMetricColumns.getLongValue(ApiConstants._SCORE_IN_PERCENTAGE, 0L);
			collectionType = itemMetricColumns.getStringValue(ApiConstants._COLLECTION_TYPE, null);
		}
		usageAsMap.put(ApiConstants.VIEWS, views);
		usageAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, score);
		usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
		usageAsMap.put(ApiConstants.TYPE, collectionType);
		return usageAsMap;
	}
	
	public List<Map<String,Object>> getCollectionActivityMetrics(String traceId, Collection<String> rowKeys,String columnFamily, Collection<String> columns, String userIds,boolean isUserIdInKey,String collectionIds, boolean userProcess) {

		List<Map<String,Object>> collectionUsageData = new ArrayList<Map<String,Object>>();
		OperationResult<Rows<String, String>> activityData = getCassandraService().readAll(traceId, columnFamily, rowKeys, columns);
		Collection<String> fetchedIds = new ArrayList<String>();
		Map<String,Set<String>> userSet = new HashMap<String,Set<String>>();
		if (!activityData.getResult().isEmpty()) {
			Rows<String, String> itemMetricRows = activityData.getResult();
			for(Row<String, String> metricRow : itemMetricRows){
				String userId = null;
				Map<String,Map<String, Object>> KeyUsageAsMap = new HashMap<String,Map<String, Object>>();
				for(String column : columns){
					Map<String,Object> usageMap = new HashMap<String,Object>();
					String[] columnPrefix = column.split(ApiConstants.TILDA);
					String[] columnMetaInfo = column.split(ApiConstants.TILDA);
					String metricName = (columnMetaInfo.length > 1) ? columnMetaInfo[columnMetaInfo.length-1] : columnMetaInfo[0];
					if((columnPrefix.length > 1 ? columnPrefix[1].matches(ApiConstants.OPTIONS_MATCH) : columnPrefix[0].matches(ApiConstants.OPTIONS_MATCH))){
						continue;
					}
					usageMap = fetchMetricData(traceId,columnMetaInfo[0],metricRow,metricName,column);
					userId = validateDefaultUser(userProcess,isUserIdInKey,userIds,userId,columnMetaInfo[0],metricRow,usageMap,userSet);
					if(KeyUsageAsMap.containsKey(columnMetaInfo[0])){
						usageMap.putAll(KeyUsageAsMap.get(columnMetaInfo[0]));
					}
					KeyUsageAsMap.put(columnMetaInfo[0], usageMap);
					if(!fetchedIds.contains(columnMetaInfo[0]) && columnMetaInfo[0].length() > 35){
						fetchedIds.add(columnMetaInfo[0]);
					}
				}
				collectionUsageData.addAll(getBaseService().convertMapToList(KeyUsageAsMap, ApiConstants.GOORUOID));
			}
		}
		
		/**
		 * Set default value at user level
		 */
		if(userProcess){
			fetchDefaultUserData(collectionIds, userSet, userIds, columns,collectionUsageData);
		}else {
			/**
			 * Set default value for collection level
			 */
			for(String id : collectionIds.split(ApiConstants.COMMA)){
				if(!fetchedIds.contains(id)){
					Map<String,Object> tempMap = setDefaultValue(id,columns);
					tempMap.put(ApiConstants.GOORUOID, id);
					collectionUsageData.add(tempMap);
				}
			}
		}
		return collectionUsageData;
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
					contentType = resourceData.getColumnByName(ApiConstants.RESOURCE_TYPE).getStringValue();
				}
				if (contentType != null) {
					if (contentType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
						if (collectionGooruOids.length() > 0) {
							collectionGooruOids.append(COMMA);
						}
						collectionGooruOids.append(itemGooruOid);
					} else if (contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT) || contentType.equalsIgnoreCase(ApiConstants.ASSESSMENT_SLASH_URL)) {
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

	public List<Map<String,Object>> getStudents(String traceId, String classId){
		
		OperationResult<ColumnList<String>> usersData = getCassandraService().read(traceId, ColumnFamily.USER_GROUP_ASSOCIATION.getColumnFamily(), classId);
		List<Map<String,Object>> userList = new ArrayList<Map<String,Object>>();
		for(Column<String> column : usersData.getResult()){
			Map<String,Object> userMap = new HashMap<String,Object>();
			userMap.put(ApiConstants.USERUID, column.getName());
			userMap.put(ApiConstants.USER_NAME, column.getStringValue());
			userList.add(userMap);
		}
		return userList;
	} 
	
	public List<Map<String,Object>> getContentItems(String traceId,String rowKey,String type,boolean fetchMetaData){
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
			try {
				dataMap.put(ApiConstants.SEQUENCE, column.getIntegerValue());
			}catch(Exception e) {
				dataMap.put(ApiConstants.SEQUENCE, column.getLongValue());
			}
			dataMap.put(ApiConstants.GOORUOID, column.getName());
			contentItems.add(dataMap);
		}
		return contentItems;
	}

	private ResponseParamDTO<Map<String, Object>> getAllStudentProgressByUnit(String traceId, String classId, String courseId, boolean isSecure) throws Exception {

		List<Map<String, Object>> unitDetails = new ArrayList<Map<String, Object>>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();

		OperationResult<ColumnList<String>> unitData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), courseId);
		ColumnList<String> units = unitData.getResult();

		OperationResult<ColumnList<String>> userGroupAssociation = getCassandraService().read(traceId, ColumnFamily.USER_GROUP_ASSOCIATION.getColumnFamily(), classId);
		ColumnList<String> userGroup = userGroupAssociation.getResult();

		if (!units.isEmpty() && units.size() > 0 && !userGroup.isEmpty() && userGroup.size() > 0) {

			for (Column<String> user : userGroup) {
				Map<String, Object> userMetadataDetails = new HashMap<String, Object>();
				/**
				 * Get activity details
				 */
				userMetadataDetails.put(ApiConstants.USERUID, user.getName());
				userMetadataDetails.put(ApiConstants.USER_NAME, user.getStringValue());

				List<Map<String, Object>> userUnitUsageDetails = new ArrayList<Map<String, Object>>();
				for (String unitGooruOid : units.getColumnNames()) {
					Map<String, Object> unitDataAsMap = new HashMap<String, Object>();
					/**
					 * Get Resource Meta data
					 */
					Collection<String> resourceColumns = new ArrayList<String>();
					resourceColumns.add(ApiConstants.TITLE);
					resourceColumns.add(ApiConstants.RESOURCE_TYPE);
					OperationResult<ColumnList<String>> unitMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), unitGooruOid, resourceColumns);
					if (!unitMetaData.getResult().isEmpty() && unitMetaData.getResult().size() > 0) {
						ColumnList<String> unitMetaDataColumns = unitMetaData.getResult();
						unitDataAsMap.put(ApiConstants.GOORUOID, unitGooruOid);
						unitDataAsMap.put(ApiConstants.TITLE, unitMetaDataColumns.getStringValue(ApiConstants.TITLE, null));
						unitDataAsMap.put(ApiConstants.TYPE, unitMetaDataColumns.getStringValue(ApiConstants.RESOURCE_TYPE, null));
						unitDataAsMap.put(SEQUENCE, units.getLongValue(unitGooruOid, 0L));

						Long scoreInPercentage = 0L;
						OperationResult<ColumnList<String>> unitActivityDetails = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
								baseService.appendTilda(classId, courseId, unitGooruOid, user.getName()));
						if (unitActivityDetails != null) {
							ColumnList<String> unitActivity = unitActivityDetails.getResult();
							if (!unitActivity.isEmpty()) {
								scoreInPercentage = unitActivity.getLongValue(ApiConstants._SCORE_IN_PERCENTAGE, 0L);
							}
						}
						unitDataAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, scoreInPercentage);
					}
					userUnitUsageDetails.add(unitDataAsMap);
				}
				userMetadataDetails.put(ApiConstants.USAGE_DATA, userUnitUsageDetails);
				unitDetails.add(userMetadataDetails);
			}
		}
		responseParamDTO.setContent(unitDetails);
		return responseParamDTO;
	}

	@Override
	public ResponseParamDTO<Map<String, Object>> getSessionStatus(String traceId, String sessionId, String contentGooruId, String collectionType, boolean isSecure) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, Object> sessionDataMap = new HashMap<String, Object>();

		OperationResult<ColumnList<String>> sessionDetails = getCassandraService().read(traceId, ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionId);
		if (sessionDetails != null && !sessionDetails.getResult().isEmpty()) {
			ColumnList<String> sessionList = sessionDetails.getResult();
			String status = sessionList.getStringValue(baseService.appendTilda(contentGooruId,STATUS), null);
			sessionDataMap.put(ApiConstants.SESSIONID, sessionId);
			if (status != null) {
				status = status.equalsIgnoreCase(ApiConstants.STOP) ? ApiConstants.COMPLETED : ApiConstants.INPROGRESS;
			} else {
				ValidationUtils.rejectInvalidRequest(ErrorCodes.E109, contentGooruId);
			}
			sessionDataMap.put(STATUS, status);
			responseParamDTO.setMessage(sessionDataMap);
			;
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E110, sessionId);
		}
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> findUsageAvailable(String traceId, String classGooruId ,String courseGooruId,String unitGooruId,String lessonGooruId,String contentGooruId) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		String key = null;
		if (StringUtils.isNotBlank(classGooruId)) {
			key = baseService.appendTilda(courseGooruId,unitGooruId,lessonGooruId,contentGooruId);
		} else {
			key = classGooruId;
		}
		OperationResult<ColumnList<String>> sessions = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), key);
		Map<String, Object> sessionDataMap = new HashMap<String, Object>();
		sessionDataMap.put(STATUS, false);
		if (sessions != null) {
			ColumnList<String> sessionList = sessions.getResult();
			if (!sessionList.isEmpty()) {
				sessionDataMap.put(STATUS, true);
			}
		}
		responseParamDTO.setMessage(sessionDataMap);
		return responseParamDTO;
	}
		
	
	public ResponseParamDTO<Map<String,Object>> getStudentsCollectionData(String traceId, String classId, String courseId, String unitId, String lessonId, String collectionId, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
		//list the resource and inside of it convert the user as usage data
		List<Map<String,Object>> resources = getContentItems(traceId,collectionId,null,true);
		List<Map<String,Object>> students = getStudents(traceId, classId);
		StringBuffer resourceIds = getBaseService().exportData(resources, ApiConstants.GOORUOID);
		StringBuffer userIds = getBaseService().exportData(students, ApiConstants.USERUID);
		
		Set<String> columnSuffix = new HashSet<String>();
		columnSuffix.add(ApiConstants.VIEWS);
		columnSuffix.add(ApiConstants._TIME_SPENT);
		columnSuffix.add(ApiConstants.SCORE);
		columnSuffix.add(ApiConstants._ANSWER_OBJECT);
		columnSuffix.add(ApiConstants.CHOICE);
		columnSuffix.add(ApiConstants.ATTEMPTS);
		columnSuffix.add(ApiConstants._AVG_TIME_SPENT);
		columnSuffix.add(ApiConstants._TIME_SPENT);
		columnSuffix.add(ApiConstants._AVG_REACTION);
		columnSuffix.add(ApiConstants.RA);
		columnSuffix.add(ApiConstants.REACTION);
		columnSuffix.add(ApiConstants.OPTIONS);
		columnSuffix.add(options.A.name());
		columnSuffix.add(options.B.name());
		columnSuffix.add(options.C.name());
		columnSuffix.add(options.D.name());
		columnSuffix.add(options.E.name());
		columnSuffix.add(options.F.name());
		
		/**
		 * Fetch session data
		 */
		Collection<String> rowKeys = getBaseService().appendAdditionalField(ApiConstants.TILDA, getBaseService().appendTilda(SessionAttributes.RS.getSession(),classId,courseId,unitId,lessonId,collectionId), userIds.toString());
		Collection<String> columns = getBaseService().appendAdditionalField(ApiConstants.TILDA, resourceIds.toString(), columnSuffix);
		columns.add(ApiConstants.GOORU_UID);
		List<String> sessionIds = getSessions(traceId,rowKeys);
		List<Map<String,Object>> assessmentUsage = getCollectionActivityMetrics(traceId, sessionIds,ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), columns, userIds.toString(),false,resourceIds.toString(),true);
		assessmentUsage = getBaseService().includeDefaultData(assessmentUsage, students, ApiConstants.GOORUOID, ApiConstants.USERUID);
//		assessmentUsage = getBaseService().LeftJoin(assessmentUsage, students, ApiConstants.USERUID, ApiConstants.USERUID);
		assessmentUsage = getBaseService().groupDataDependOnkey(assessmentUsage,ApiConstants.GOORUOID,ApiConstants.USAGE_DATA);
		assessmentUsage = getBaseService().LeftJoin(resources,assessmentUsage,ApiConstants.GOORUOID,ApiConstants.GOORUOID);
		
		/**
		 * Setting of assessment meta info may be tricky
		 */
		List<Map<String,Object>> assessmentMetaInfo = getQuestionMetaData(traceId,collectionId);
		assessmentUsage = getBaseService().LeftJoin(assessmentUsage, assessmentMetaInfo, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
		
		responseParamDTO.setContent(assessmentUsage);
		return responseParamDTO;
	}
	
	public List<String> getSessions(String traceId, Collection<String> rowKeys) {
		
		List<String> sessions = new ArrayList<String>();
		OperationResult<Rows<String, String>> sessionItems = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), rowKeys);
		if(!sessionItems.getResult().isEmpty()){
			for(Row<String,String> sessionRow : sessionItems.getResult()) {
				String sessionId = sessionRow.getColumns().getStringValue(ApiConstants._SESSION_ID, null);
				if(sessionId != null){
					sessions.add(sessionId);
				}
			}
		}
		return sessions;
	}
	
	private List<Map<String,Object>> getQuestionMetaData(String traceId,String collectonId){
		Collection<String> columns = new ArrayList<String>();
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		OperationResult<Rows<String, String>> questionMetaDatas = getCassandraService().readAll(traceId, ColumnFamily.ASSESSMENT_ANSWER.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, collectonId, columns);
		Map<String,Object> resultMap = new HashMap<String,Object>();
		for(Row<String, String> row : questionMetaDatas.getResult()){
			
			Map<String,Object> dataMap = new HashMap<String,Object>();
			String key = row.getColumns().getStringValue(ApiConstants.QUESTION_GOORU_OID, null);
			dataMap.put(ApiConstants._QUESTIONGOORUOID, row.getColumns().getStringValue(ApiConstants.QUESTION_GOORU_OID, null));
			dataMap.put(ApiConstants.QUESTION_TYPE, row.getColumns().getStringValue(ApiConstants._QUESTION_TYPE, null));
			dataMap.put(ApiConstants.SEQUENCE, row.getColumns().getIntegerValue(ApiConstants.SEQUENCE, null));
			dataMap.put(ApiConstants._ISCORRECT, row.getColumns().getIntegerValue(ApiConstants.IS_CORRECT, null));
			dataMap.put(ApiConstants._ANSWERTEXT, row.getColumns().getStringValue(ApiConstants.ANSWER_TEXT, null));
			Map<String,Object> tempMap = new HashMap<String,Object>();
			if(resultMap.containsKey(key)){
				Map<String,Object> storedData = (Map<String, Object>) resultMap.get(key);
				List<Map<String,Object>> dataList = (List<Map<String, Object>>) storedData.get(ApiConstants.META_DATA);
				dataList.add(dataMap);
				tempMap.put(ApiConstants.META_DATA, dataList);
			}else{
				List<Map<String,Object>> dataList = new ArrayList<Map<String,Object>>();
				dataList.add(dataMap);
				tempMap.put(ApiConstants.META_DATA, dataList);
				
			}
			resultMap.put(row.getColumns().getStringValue(ApiConstants.QUESTION_GOORU_OID, null), tempMap);
		}
		return getBaseService().convertMapToList(resultMap, ApiConstants.GOORUOID);
	}

	private Map<String, Object> setDefaultValue(String id,
			Collection<String> columns) {
		Map<String, Object> usageMap = new HashMap<String, Object>();
		for (String metricName : columns) {
			
			if (metricName.endsWith(ApiConstants._COLLECTION_TYPE)) {
				usageMap.put(ApiConstants.COLLECTION_TYPE, null);
			} else if (metricName.endsWith(ApiConstants.VIEWS)) {
				usageMap.put(ApiConstants.VIEWS, 0L);
			} else if (metricName.endsWith(ApiConstants._SCORE_IN_PERCENTAGE)) {
				usageMap.put(ApiConstants.SCORE_IN_PERCENTAGE, 0L);
			} else if (metricName.endsWith(ApiConstants._TIME_SPENT)) {
				usageMap.put(ApiConstants.TIMESPENT, 0L);
			} else if (metricName.endsWith(ApiConstants._ANSWER_OBJECT)) {
				usageMap.put(ApiConstants.ANSWER_OBJECT, null);
			}else if(metricName.endsWith(ApiConstants.ATTEMPTS)){
				usageMap.put(ApiConstants.ATTEMPTS, 0L);
			} else if (metricName.endsWith(ApiConstants._AVG_TIME_SPENT)) {
				usageMap.put(ApiConstants.AVG_TIME_SPENT, 0L);
			} else if (metricName.endsWith(ApiConstants.CHOICE)) {
				usageMap.put(ApiConstants.TEXT, null);
			}else if(metricName.equalsIgnoreCase(ApiConstants.RA)){
				usageMap.put(ApiConstants.REACTION, 0L);
			}else if(metricName.equalsIgnoreCase(ApiConstants._AVG_REACTION)){
				usageMap.put(ApiConstants.AVG_REACTION, 0L);
			} else if (metricName.endsWith(ApiConstants._TIME_SPENT)) {
				usageMap.put(ApiConstants.TIMESPENT, 0L);
			} else if (metricName.endsWith(ApiConstants.OPTIONS)) {
				usageMap.put(ApiConstants.OPTIONS, null);
			}
		}
		return usageMap;
	}
	
	private Map<String,Object> fetchMetricData(String traceId, String id,Row<String, String> metricRow, String metricName,String column){
		Map<String,Object> usageMap = new HashMap<String,Object>();
 		if(metricName.equalsIgnoreCase(ApiConstants._COLLECTION_TYPE)){
			usageMap.put(ApiConstants.COLLECTION_TYPE, metricRow.getColumns().getStringValue(column, null));
		}else if(metricName.equalsIgnoreCase(ApiConstants.VIEWS)){
			usageMap.put(ApiConstants.VIEWS, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equalsIgnoreCase(ApiConstants._SCORE_IN_PERCENTAGE)){
			usageMap.put(ApiConstants.SCORE_IN_PERCENTAGE, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equalsIgnoreCase(ApiConstants._TIME_SPENT)){
			usageMap.put(ApiConstants.TIMESPENT, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if (metricName.endsWith(ApiConstants._AVG_TIME_SPENT)) {
			usageMap.put(ApiConstants.AVG_TIME_SPENT, metricRow.getColumns().getLongValue(column.trim(), 0L));
		} else if(metricName.equalsIgnoreCase(ApiConstants._ANSWER_OBJECT)){
			usageMap.put(ApiConstants.ANSWER_OBJECT, metricRow.getColumns().getStringValue(column.trim(), null));
		}else if(metricName.equalsIgnoreCase(ApiConstants.ATTEMPTS)){
			usageMap.put(ApiConstants.ATTEMPTS, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equalsIgnoreCase(ApiConstants.CHOICE)){
			usageMap.put(ApiConstants.TEXT, metricRow.getColumns().getStringValue(column.trim(), null));
		}else if(metricName.equalsIgnoreCase(ApiConstants.RA)){
			usageMap.put(ApiConstants.REACTION, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equalsIgnoreCase(ApiConstants._AVG_REACTION)){
			usageMap.put(ApiConstants.AVG_REACTION, metricRow.getColumns().getStringValue(column.trim(), null));
		}else if(metricName.matches(ApiConstants.OPTIONS)){
			Map<String,Long> optionMap = new HashMap<String,Long>();
			optionMap.put(ApiConstants.options.A.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.A.name()), 0L));
			optionMap.put(ApiConstants.options.B.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.B.name()), 0L));
			optionMap.put(ApiConstants.options.C.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.C.name()), 0L));
			optionMap.put(ApiConstants.options.D.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.D.name()), 0L));
			optionMap.put(ApiConstants.options.E.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.E.name()), 0L));
			optionMap.put(ApiConstants.options.F.name(), metricRow.getColumns().getLongValue(getBaseService().appendTilda(id,ApiConstants.options.F.name()), 0L));
			usageMap.put(ApiConstants.OPTIONS,optionMap);
		}else if(metricName.equalsIgnoreCase(ApiConstants.GOORU_UID)){
			usageMap.put(ApiConstants.USERUID, metricRow.getColumns().getStringValue(ApiConstants.GOORU_UID, null));
		}else {
			try{
				usageMap.put(metricName, metricRow.getColumns().getLongValue(column, 0L));
			}catch(Exception e){
				InsightsLogger.error(traceId, getBaseService().errorHandler(ErrorMessages.UNHANDLED_EXCEPTION, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), column),e);
			}
		}
 		return usageMap;
	}
	
	private void fetchDefaultUserData(String collectionIds, Map<String,Set<String>> userSet, String userIds,Collection<String> columns, List<Map<String,Object>> collectionUsageData){
	
		for(String collectionId : collectionIds.split(ApiConstants.COMMA)){
			
			Map<String,Object> tempMap = setDefaultValue(collectionId,columns);
			tempMap.put(ApiConstants.GOORUOID, collectionId);
			if(userSet.containsKey((collectionId))){
				insertDefaultUserData(userSet,collectionId,userIds,tempMap,collectionUsageData);
			}else{
				userSet.put(collectionId, new HashSet<String>());
				insertDefaultUserData(userSet,collectionId,userIds,tempMap,collectionUsageData);
			}
		}
	}
	
	private void insertDefaultUserData(Map<String,Set<String>> userSet,String collectionId,String userIds,Map<String,Object> tempMap, List<Map<String,Object>> collectionUsageData){
		Set<String> temp = userSet.get(collectionId);
		for(String user : userIds.split(ApiConstants.COMMA)){
			Map<String,Object> insertableMap = new HashMap<String,Object>(tempMap);
			if(!temp.contains(user)){
				insertableMap.put(ApiConstants.USERUID, user);
				collectionUsageData.add(insertableMap);
			}
		}
	}
	private String validateDefaultUser(boolean userProcess,boolean isUserIdInKey,String userIds,String userId,String collectionId,Row<String,String> metricRow,Map<String,Object> usageMap,Map<String,Set<String>> userSet){
		
	if(userId == null){
		if(isUserIdInKey){
			for(String id : userIds.split(ApiConstants.COMMA)){
				if(metricRow.getKey().contains(id)){
					userId = id;
					break;
				}
		}
		}else{
			if(metricRow.getColumns().getColumnNames().contains(ApiConstants.GOORU_UID)){
				userId = metricRow.getColumns().getStringValue(ApiConstants.GOORU_UID, null);
			}
		}
	}
	
	usageMap.put(ApiConstants.USERUID, userId);
	if(userProcess){
	Set<String>	set = new HashSet<String>();
	set.add(userId);
	if(userSet.containsKey(collectionId)){
		set.addAll(userSet.get(collectionId));
	}
	userSet.put(collectionId, set);
	}
	return userId;
	
	}

	public ResponseParamDTO<Map<String, Object>> getStudentAssessmentSummary(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentId, String userUid,
			String sessionId, boolean isSecure) throws Exception {

		List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		if ((sessionId != null && StringUtils.isNotBlank(sessionId.trim())) && (classId != null && StringUtils.isNotBlank(classId.trim())) && (courseId != null && StringUtils.isNotBlank(courseId.trim())) 
				&& (unitId != null && StringUtils.isNotBlank(unitId.trim())) && (lessonId != null && StringUtils.isNotBlank(lessonId.trim()))) {
			List<Map<String, Object>> unitColumnResult = getContentItems(traceId, courseId, null, false);
			for (Map<String, Object> unit : unitColumnResult) {
				String unitGooruId = unit.get(ApiConstants.GOORUOID).toString();
				if (unitGooruId.equalsIgnoreCase(unitId)) {
					List<Map<String, Object>> lessonColumnResult = getContentItems(traceId, unitGooruId, null, false);
					for (Map<String, Object> lesson : lessonColumnResult) {
						String lessonGooruId = lesson.get(ApiConstants.GOORUOID).toString();
						if (lessonGooruId.equalsIgnoreCase(lessonId)) {
							List<Map<String, Object>> collectionColumnResult = getContentItems(traceId, lessonGooruId, null, false);
							for (Map<String, Object> collection : collectionColumnResult) {
								String collectionGooruId = collection.get(ApiConstants.GOORUOID).toString();
								if (collectionGooruId.equalsIgnoreCase(assessmentId)) {
									itemDataMapAsList = getCollectionSummaryData(traceId, collectionGooruId, sessionId, itemDataMapAsList, isSecure);
									break;
								}
							}
							responseParamDTO.setContent(itemDataMapAsList);
							break;
						}
					}
					break;
				}
			}
		} else if ((sessionId != null && StringUtils.isNotBlank(sessionId.trim())) && (assessmentId != null && StringUtils.isNotBlank(assessmentId.trim()))) {
			itemDataMapAsList = getCollectionSummaryData(traceId, assessmentId, sessionId, itemDataMapAsList, isSecure);
			responseParamDTO.setContent(itemDataMapAsList);
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E111, getBaseService().appendComma("assessmentId", "sessionId"),
					getBaseService().appendComma("classId", "courseId", "unitId", "lessonId", "assessmentId"));
		}
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> getCollectionSummaryData(String traceId, String collectionGooruId, String sessionId, List<Map<String, Object>> itemDataMapAsList, boolean isSecure) {
		List<Map<String, Object>> itemColumnResult = getContentItems(traceId, collectionGooruId, null, false);
		List<Map<String, Object>> rawDataMapAsList = new ArrayList<Map<String, Object>>();
		StringBuffer itemGooruOids = getBaseService().exportData(itemColumnResult, ApiConstants.GOORUOID);

		//Resource metadata
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		resourceColumns.add(ApiConstants.RESOURCE_FORMAT);
		resourceColumns.add(ApiConstants.CATEGORY);
		resourceColumns.add(ApiConstants.HAS_FRAME_BREAKER);
		resourceColumns.add("question.type");
		resourceColumns.add("question.questionType");
		rawDataMapAsList = getResourceData(traceId, isSecure, rawDataMapAsList, itemGooruOids.toString(), resourceColumns, ApiConstants.RESOURCE);
		
		//Usage Data
		Set<String> columnSuffix = DataUtils.getSessionActivityMetricsMap().keySet();
		Collection<String> columns = getBaseService().appendAdditionalField(ApiConstants.TILDA, itemGooruOids.toString(), columnSuffix);
		List<Map<String,Object>> usageDataList = getSessionActivityMetrics(traceId, getBaseService().convertStringToCollection(sessionId), ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), columns, itemGooruOids.toString());
		//List<Map<String,Object>> usageDataList = getCollectionActivityMetrics(traceId, getBaseService().convertStringToCollection(sessionId), ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), columns, null, false, itemGooruOids.toString(), false);

		//Question meta 
		List<Map<String,Object>> answerRawData = getBaseService().getQuestionAnswerData(
				getBaseService().getRowsColumnValues(traceId,
						getCassandraService().readAll(traceId, ColumnFamily.ASSESSMENT_ANSWER.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, collectionGooruId,
								new ArrayList<String>())), ApiConstants.QUESTION_GOORU_OID);

		itemDataMapAsList = getBaseService().LeftJoin(rawDataMapAsList, usageDataList, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
		itemDataMapAsList = getBaseService().LeftJoin(itemDataMapAsList, answerRawData, ApiConstants.GOORUOID, ApiConstants.QUESTION_GOORU_OID);
		
		/**
		 * get teacher response
		 */
		StringBuffer teacherUId = new StringBuffer();
		teacherUId = getBaseService().exportData(itemDataMapAsList, ApiConstants.FEEDBACKPROVIDER);
		if (!teacherUId.toString().isEmpty()) {
			String teacherUid = "";
			String[] teacherid = teacherUId.toString().split(COMMA);
			for (String ids : teacherid) {
				if (ids != null && !ids.isEmpty() && !ids.contains("null")) {
					teacherUid = ids;
					break;
				}
			}
			Map<String, String> selectValues = new HashMap<String, String>();
			selectValues.put(ApiConstants.FEEDBACK_TEACHER_NAME, ApiConstants.USERNAME);
			selectValues.put(ApiConstants.FEEDBACKPROVIDER, ApiConstants.GOORUUID);
			List<Map<String, Object>> teacherData = getBaseService().getColumnValues(traceId, getCassandraService().getClassPageUsage(traceId, ColumnFamily.USER.getColumnFamily(), "", teacherUid, "", new ArrayList<String>()));
			teacherData = getBaseService().properName(teacherData, selectValues);
			itemDataMapAsList = getBaseService().LeftJoin(itemDataMapAsList, teacherData, ApiConstants.FEEDBACKPROVIDER, ApiConstants.FEEDBACKPROVIDER);
		}
		return itemDataMapAsList;
	}
	
	private List<Map<String, Object>> getSessionActivityMetrics(String traceId, Collection<String> rowKeys, String columnFamily, Collection<String> columns, String itemGooruOids) {
		List<Map<String, Object>> outputUsageDataAsList = new ArrayList<Map<String, Object>>();
		OperationResult<Rows<String, String>> activityData = getCassandraService().readAll(traceId, columnFamily, rowKeys, columns);
		if (!activityData.getResult().isEmpty()) {
			Rows<String, String> itemMetricRows = activityData.getResult();
			for (Row<String, String> metricRow : itemMetricRows) {
				Map<String, Map<String, Object>> KeyUsageAsMap = new HashMap<String, Map<String, Object>>();
				if (!metricRow.getColumns().isEmpty() && metricRow.getColumns().size() > 0) {
					for (String item : itemGooruOids.split(COMMA)) {
						Map<String, Object> usageMap = new HashMap<String, Object>();
						Map<String, Object> optionsAsMap = new HashMap<String, Object>();
						for (String column : columns) {
							if (column.contains(item)) {
								String[] columnMetaInfo = column.split(ApiConstants.TILDA);
								String metricName = (columnMetaInfo.length > 1) ? columnMetaInfo[columnMetaInfo.length - 1] : columnMetaInfo[0];
								if (DataUtils.getStringColumns().containsKey(metricName)) {
									usageMap.put(DataUtils.getStringColumns().get(metricName), metricRow.getColumns().getStringValue(column.trim(), null));
								} else if (metricName.matches(ApiConstants.OPTIONS_MATCH) && metricRow.getColumns().getColumnNames().contains(column.trim())) {
									optionsAsMap.put(metricName, metricRow.getColumns().getLongValue(column.trim(), 0L));
								} else if (DataUtils.getLongColumns().containsKey(metricName) && !metricName.matches(ApiConstants.OPTIONS_MATCH)) {
									usageMap.put(DataUtils.getLongColumns().get(metricName), metricRow.getColumns().getLongValue(column.trim(), 0L));
								}
							}	                                                
						}
						usageMap.put(ApiConstants.OPTIONS, optionsAsMap);
						KeyUsageAsMap.put(item, usageMap);
					}
				}
				outputUsageDataAsList.addAll(getBaseService().convertMapToList(KeyUsageAsMap, ApiConstants.GOORUOID));
			}
		}
		return outputUsageDataAsList.isEmpty() ? null : outputUsageDataAsList;
	}
	
}
