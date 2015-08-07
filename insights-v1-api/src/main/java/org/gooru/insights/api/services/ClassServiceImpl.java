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
import org.gooru.insights.api.constants.ErrorCodes;
import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.utils.DataUtils;
import org.gooru.insights.api.utils.InsightsLogger;
import org.gooru.insights.api.utils.ServiceUtils;
import org.gooru.insights.api.utils.ValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class ClassServiceImpl implements ClassService, InsightsConstant {
	
	private static final Logger logger = LoggerFactory.getLogger(ClassServiceImpl.class);
	
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
	
	public ResponseParamDTO<Map<String,Object>> getAllStudentsUnitUsage(String traceId, String classId, String courseId, String unitId, String studentId, String collectionType, Boolean getUsageData, boolean isSecure) throws Exception {
		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String,Object>> studentsMetaData = null;
		List<Map<String,Object>> resultData = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> lessonsRawData = new ArrayList<Map<String, Object>>();

		//fetch list of lessons
		OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitId);
		ColumnList<String> lessons = lessonData.getResult();
		
		//fetch metadata of lesson
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		resourceColumns.add(ApiConstants._GOORUOID);
		String lessonGooruOIds = getBaseService().convertListToString(lessons.getColumnNames());

			
		if(StringUtils.isNotBlank(lessonGooruOIds)){
			lessonsRawData = getResourceData(traceId, isSecure, lessonGooruOIds, resourceColumns, ApiConstants.LESSON);
		}
		responseParamDTO.setContent(lessonsRawData);
		
		//fetch usage data of lesson
		if(getUsageData) {
			
			//Get list of students
			studentsMetaData = getStudents(traceId,classId);

			if(!studentsMetaData.isEmpty() || StringUtils.isNotBlank(studentId)){
				
				if(StringUtils.isBlank(studentId)){
					studentId = getBaseService().getCommaSeparatedIds(studentsMetaData, ApiConstants.USER_UID).toString();
				}
				for(Map<String, Object> lessonrawData : lessonsRawData) {
	
					String lessonGooruOid = lessonrawData.get(ApiConstants.GOORUOID).toString();
					OperationResult<ColumnList<String>> assessmentData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
					ColumnList<String> assessments = assessmentData.getResult();
					if (lessons != null) {
						lessonrawData.put(ApiConstants.SEQUENCE, lessons.getColumnByName(lessonGooruOid) != null ? lessons.getLongValue(lessonGooruOid, 0L) : 0L);
					}
					lessonrawData.put(ApiConstants.ASSESSMENT_COUNT, assessments.size());
					lessonrawData.put(ApiConstants.GOORUOID, lessonGooruOid);
	
					//fetch lesson usage data
					String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
					
					String contentType = null;
					if(collectionType !=null){
						if(collectionType.equalsIgnoreCase(ApiConstants.ASSESSMENT)){
							contentType = ApiConstants.ASSESSMENT_TYPES;
						}else if(collectionType.matches(ApiConstants.COLLECTION_MATCH)) {
							contentType = ApiConstants.COLLECTION_MATCH;
						}
					}
					List<Map<String,Object>> contentsMetaData = getContentItems(traceId,lessonGooruOid,contentType,true,null,DataUtils.getResourceFields());
					if(!contentsMetaData.isEmpty()){
						Set<String> columnSuffix = new HashSet<String>();
						columnSuffix.add(ApiConstants._TIME_SPENT);
						columnSuffix.add(ApiConstants._SCORE_IN_PERCENTAGE);
						columnSuffix.add(ApiConstants.VIEWS);
						StringBuffer collectionIds = getBaseService().getCommaSeparatedIds(contentsMetaData, ApiConstants.GOORUOID);
						Collection<String> rowKeys = ServiceUtils.generateCommaSeparatedStringToKeys(ApiConstants.TILDA,classLessonKey, studentId);
						Collection<String> columns = ServiceUtils.generateCommaSeparatedStringToKeys(ApiConstants.TILDA,collectionIds.toString(), columnSuffix);
						/**
						 * Get collection activity
						 */
						List<Map<String,Object>> contentUsage = getIdSeparatedMetrics(traceId, rowKeys,ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), columns, studentId,true,collectionIds.toString(),true);
						contentUsage = getBaseService().leftJoin(contentUsage,contentsMetaData,ApiConstants.GOORUOID,ApiConstants.GOORUOID);
						//group at content level
						contentUsage = getBaseService().groupRecordsBasedOnKey(contentUsage,ApiConstants.USER_UID,ApiConstants.USAGE_DATA);
						//Inject Lesson data
						contentUsage = getBaseService().injectMapRecord(contentUsage, lessonrawData);
						resultData.addAll(contentUsage);
					}
				}
				//group at user level
				resultData = getBaseService().groupRecordsBasedOnKey(resultData,ApiConstants.USER_UID,ApiConstants.USAGE_DATA);
				resultData = getBaseService().leftJoin(resultData,studentsMetaData,ApiConstants.USER_UID,ApiConstants.USER_UID);
				responseParamDTO.setContent(resultData);
			}
		}
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> getCoursePlan(String traceId, String classId, String courseId, String userUid, boolean isSecure) throws Exception {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> unitDataMapAsList = new ArrayList<Map<String, Object>>();
		OperationResult<ColumnList<String>>  classMetaData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
		Long classMinScore = 0L;
		if(classMetaData != null & classMetaData.getResult() != null && !classMetaData.getResult().isEmpty()) {
			classMinScore = classMetaData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
		}
		
		OperationResult<ColumnList<String>> unitData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), courseId);
		if (unitData != null) {
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
				unitDataAsMap.put(ApiConstants.SEQUENCE, unit.getLongValue());
				
				OperationResult<ColumnList<String>> unitMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), unitGooruOid, resourceColumns);
				if (!unitMetaData.getResult().isEmpty() && unitMetaData.getResult().size() > 0) {
					ColumnList<String> unitMetaDataColumns = unitMetaData.getResult();
					unitDataAsMap.put(ApiConstants.TITLE, unitMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
				}

				OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitGooruOid);
				ColumnList<String> lessons = lessonData.getResult();
				for (Column<String> lesson : lessons) {
					long assessmentCount = 0; long scoreMet = 0; long scoreNotMet = 0; long assessmentAttempted = 0; long collectionNotAttempted = 0; long assessmentNotAttempted = 0;
					Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
					String lessonGooruOid = lesson.getName();
					String classLessonKey = getBaseService().appendTilda(classId, courseId, unitGooruOid, lessonGooruOid);
					if (StringUtils.isNotBlank(userUid)) {
						classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
					}
					OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
					lessonDataAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
					lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);
					lessonDataAsMap.put(ApiConstants.SEQUENCE, lesson.getLongValue());
					
					if (!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
						ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
						lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
					}
					// fetch lesson item score status
					OperationResult<ColumnList<String>> lessonMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey);
					ColumnList<String> lessonMetricColumnList = null;
					if (lessonMetricsData != null && !lessonMetricsData.getResult().isEmpty()) {
						lessonMetricColumnList = lessonMetricsData.getResult();
					}
					OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
					ColumnList<String> items = itemData.getResult();
					for (Column<String> item : items) {
						String itemGooruOid = item.getName();
						Long assessmentScore = null;

						// fetch type
						Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
						getResourceMeta(itemDataAsMap, isSecure, traceId, itemGooruOid, getBaseService().convertStringToCollection(ApiConstants.RESOURCE_TYPE));

						String itemType = null;
						if (itemDataAsMap.get(ApiConstants.TYPE) != null && StringUtils.isNotBlank(itemDataAsMap.get(ApiConstants.TYPE).toString())) {
							itemType = itemDataAsMap.get(ApiConstants.TYPE).toString();
						}
						if (itemType != null) {
							if (itemType.matches(ApiConstants.ASSESSMENT_TYPES)) {
								assessmentCount++;
								if (lessonMetricColumnList != null && lessonMetricColumnList.size() > 0) {
									assessmentScore = lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants._SCORE_IN_PERCENTAGE), null);
									if (assessmentScore != null && assessmentScore >= classMinScore) {
										scoreMet++;
										assessmentAttempted++;
									} else if (assessmentScore != null && assessmentScore < classMinScore) {
										scoreNotMet++;
										assessmentAttempted++;
										lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_NOT_MET);
										break;
									}
								}
								if (assessmentScore == null) {
									assessmentNotAttempted++;
								}
							} else if (itemType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
								collectionNotAttempted++;
							}
						}
					}
					if (assessmentAttempted == 0 || (collectionNotAttempted == items.size())) {
						lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.NOT_ATTEMPTED);
					} else if (scoreMet > 0 && scoreNotMet == 0 && assessmentNotAttempted == 0 && (scoreMet == assessmentCount)) {
						lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_MET);
					} else if (assessmentAttempted > 0 && (scoreMet < assessmentCount)) {
						lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.SCORE_NOT_MET);
					} else if (collectionNotAttempted > 0) {
						lessonDataAsMap.put(ApiConstants.SCORE_STATUS, ApiConstants.NOT_ATTEMPTED);
					}
					lessonDataMapAsList.add(lessonDataAsMap);
				}
				unitDataAsMap.put(ApiConstants.ITEM, lessonDataMapAsList);
				unitDataMapAsList.add(unitDataAsMap);
			}
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
		if (classData != null && !classData.getResult().isEmpty() && classData.getResult().size() > 0) {
			classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
		}
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.GOORUOID);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		resourceColumns.add(ApiConstants.FOLDER);
		resourceColumns.add(ApiConstants.URL);
		
		OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitId);
		if(lessonData != null) {
		ColumnList<String> lessons = lessonData.getResult();
		for (Column<String> lesson : lessons) {
			long scoreMet = 0; long scoreNotMet = 0; long assessmentAttempted = 0; long assessmentNotAttempted = 0; 
			long collectionNotViewed = 0; long collectionViewed = 0;
			long assessmentCount = 0;  
			List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
			Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
			String lessonGooruOid = lesson.getName();
			String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
			}
			lessonDataAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
			lessonDataAsMap.put(ApiConstants.SEQUENCE, lesson.getLongValue());
			
			OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
			if (lessonMetaData != null && !lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
				ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
				lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
			}
			//Fetch lesson metrics
			ColumnList<String> lessonMetricColumnList = null;
			OperationResult<ColumnList<String>> lessonMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey);
			if (lessonMetricsData != null && !lessonMetricsData.getResult().isEmpty()) {
				lessonMetricColumnList = lessonMetricsData.getResult();
			}
			//Fetch item data
			OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
			ColumnList<String> items = itemData.getResult();
			for (Column<String> item : items) {
				String itemGooruOid = item.getName();
				Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
				itemDataAsMap.put(ApiConstants.SEQUENCE, item.getLongValue());

				getResourceMeta(itemDataAsMap, isSecure, traceId, itemGooruOid, resourceColumns);
				String itemType = null;
				if (itemDataAsMap.get(ApiConstants.TYPE) != null && StringUtils.isNotBlank(itemDataAsMap.get(ApiConstants.TYPE).toString())) {
					itemType = itemDataAsMap.get(ApiConstants.TYPE).toString();
				}
				
				long itemViews = 0l;
				if(lessonMetricColumnList != null && lessonMetricColumnList.size() > 0) {
					itemViews = lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid,ApiConstants.VIEWS), 0L);
				}
				String itemScoreStatus = null;
				if (itemType != null) {
                    if (itemType.matches(ApiConstants.ASSESSMENT_TYPES)) {
						assessmentCount++;
						Long assessmentScore = null;
						if (lessonMetricColumnList != null && lessonMetricColumnList.size() > 0) {
							assessmentScore = lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid,ApiConstants._SCORE_IN_PERCENTAGE), null);
							if (assessmentScore != null && assessmentScore >= classMinScore) {
								scoreMet++;
								assessmentAttempted++;
								itemScoreStatus = ApiConstants.SCORE_MET;
							} else if (assessmentScore != null && assessmentScore < classMinScore) {
								scoreNotMet++;
								assessmentAttempted++;
								itemScoreStatus = ApiConstants.SCORE_NOT_MET;
							}
						}
						if (assessmentScore == null) {
							if (assessmentAttempted == 0) {
								assessmentNotAttempted++;
							}
							itemScoreStatus = ApiConstants.NOT_ATTEMPTED;
						}
					} else if (itemType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
						if(itemViews > 0) {
							collectionViewed++;
							itemScoreStatus = ApiConstants.VIEWED;
						} else {
							collectionNotViewed++;
							itemScoreStatus = ApiConstants.NOT_VIEWED;
						}
					}
				}
				itemDataAsMap.put(ApiConstants.VIEWS, itemViews);					
				itemDataAsMap.put(ApiConstants.GOORUOID, itemGooruOid);
				itemDataAsMap.put(ApiConstants.SCORE_STATUS, itemScoreStatus);
				itemDataMapAsList.add(itemDataAsMap);
			}
			lessonDataAsMap.put(ApiConstants.ITEM, itemDataMapAsList);
			String lessonScoreStatus = null;
			if (assessmentAttempted == 0 && (itemDataMapAsList.size() == assessmentNotAttempted)) {
				lessonScoreStatus = ApiConstants.NOT_ATTEMPTED;
			} else if(scoreNotMet > 0 || (assessmentAttempted > 0 && (assessmentCount != 0L && scoreMet < assessmentCount))) { 
				lessonScoreStatus = ApiConstants.SCORE_NOT_MET;
			} else if (scoreMet > 0 && scoreNotMet == 0 && assessmentNotAttempted == 0 && (assessmentCount != 0L && scoreMet == assessmentCount)) {
				lessonScoreStatus = ApiConstants.SCORE_MET;
			} else if(assessmentNotAttempted > 0) {
				lessonScoreStatus = ApiConstants.NOT_ATTEMPTED;
			} else if(collectionViewed == items.size() && collectionNotViewed == 0) {
				lessonScoreStatus = ApiConstants.VIEWED;
			} else if((collectionNotViewed == items.size() && collectionViewed == 0) || collectionNotViewed > 0){
				lessonScoreStatus = ApiConstants.NOT_VIEWED;
			}

			lessonDataAsMap.put(ApiConstants.SCORE_STATUS, lessonScoreStatus);
			lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);
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
			if (unitData != null && !unitData.getResult().isEmpty() && unitData.getResult().size() > 0) {
				for (Column<String> unit : unitData.getResult()) {
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
					unitDataAsMap.put(ApiConstants.SEQUENCE, unit.getLongValue());

					// Fetch unit's assessment and collections count 
					long unitAssessmentCount = 0L; long unitCollectionCount = 0L;
					OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.CONTENT_META.getColumnFamily(), unitGooruOid);
					if(!lessonData.getResult().isEmpty()){
						unitAssessmentCount = lessonData.getResult().getLongValue(ApiConstants.ASSESSMENT_COUNT, 0l);	
						unitCollectionCount = lessonData.getResult().getLongValue(ApiConstants.COLLECTION_COUNT, 0l);	
					}
					// Fetch unit's total study time & unique views of collections 
					OperationResult<ColumnList<String>> collectionMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
							getBaseService().appendTilda(classUnitKey, ApiConstants.COLLECTION));
					ColumnList<String> collectionMetricColumnList = null;
					Long collectionsViewedInUnit = 0L;
					Long unitCollectionsTotalStudyTime = 0L;
					if (collectionMetricsData != null && !collectionMetricsData.getResult().isEmpty()) {
						collectionMetricColumnList = collectionMetricsData.getResult();
						if (collectionMetricColumnList != null && collectionMetricColumnList.size() > 0) {
							collectionsViewedInUnit = collectionMetricColumnList.getLongValue(ApiConstants._UNIQUE_VIEWS, 0L);
							unitCollectionsTotalStudyTime = collectionMetricColumnList.getLongValue(ApiConstants._TIME_SPENT, 0L);
						}
					}
					
					// Fetch unit's avgScore & unique attempt count of assessments 
					OperationResult<ColumnList<String>> assessmentMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), getBaseService().appendTilda(classUnitKey, ApiConstants.ASSESSMENT));
					ColumnList<String> assessmentMetricColumnList = null;
					Long unitAvgScore = 0L;
					Long assessmentsAttemptedInUnit = 0L;
					if (assessmentMetricsData != null && !assessmentMetricsData.getResult().isEmpty()) {
						assessmentMetricColumnList = assessmentMetricsData.getResult();
						if (assessmentMetricColumnList != null && assessmentMetricColumnList.size() > 0) {
							assessmentsAttemptedInUnit = assessmentMetricColumnList.getLongValue(ApiConstants._UNIQUE_VIEWS, 0L);
							unitAvgScore = assessmentMetricColumnList.getLongValue(ApiConstants._SCORE_IN_PERCENTAGE, 0L);
						}
					}
					unitDataAsMap.put(ApiConstants.COLLECTIONS_VIEWED, collectionsViewedInUnit);
					unitDataAsMap.put(ApiConstants.TOTAL_STUDY_TIME, unitCollectionsTotalStudyTime);
					unitDataAsMap.put(ApiConstants.ASSESSMENTS_ATTEMPTED, assessmentsAttemptedInUnit);
					unitDataAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, unitAvgScore);
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
				ColumnList<String> sessionsInfo = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), baseService.appendTilda(key, INFO)).getResult();
				if (!fetchOpenSession) {
					for (Column<String> sessionColumn : sessionList) {
						if (sessionsInfo.getStringValue(baseService.appendTilda(sessionColumn.getName(), TYPE), null).equalsIgnoreCase(STOP)) {
							resultSet.add(generateSessionMap(sessionColumn.getName(), sessionColumn.getLongValue()));
						}
					}
				} else {
					for (Column<String> sessionColumn : sessionList) {
						if (sessionsInfo.getStringValue(baseService.appendTilda(sessionColumn.getName(), TYPE), null).equalsIgnoreCase(START)) {
							Map<String, Object> sessionInfoMap = generateSessionMap(sessionColumn.getName(), sessionColumn.getLongValue());
							sessionInfoMap.put(LAST_ACCESSED_RESOURCE, sessionsInfo.getStringValue(baseService.appendTilda(sessionColumn.getName(), _LAST_ACCESSED_RESOURCE), null));
							resultSet.add(sessionInfoMap);
						}
					}
				}
				resultSet = baseService.sortBy(resultSet, EVENT_TIME, ApiConstants.ASC);
			}
		}
		responseParamDTO.setContent(addSequence(resultSet));
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> addSequence(List<Map<String, Object>> resultSet) {
		List<Map<String, Object>> finalSet = null;
		if (resultSet != null) {
			int sequence = 1;
			finalSet = new ArrayList<Map<String, Object>>();
			for (Map<String, Object> resultMap : resultSet) {
				resultMap.put(SEQUENCE, sequence++);
				finalSet.add(resultMap);
			}
		}
		return finalSet;
	}
	private Map<String,Object> generateSessionMap(String sessionId,Long eventTime){
		HashMap<String, Object> session = new HashMap<String, Object>();
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
		if (classData != null && !classData.getResult().isEmpty() && classData.getResult().size() > 0) {
			classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
		}
		
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.GOORUOID);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		OperationResult<ColumnList<String>> lessonData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), unitId);
		if(lessonData != null && !lessonData.getResult().isEmpty() && lessonData.getResult().size() > 0) {
		for (Column<String> lesson : lessonData.getResult()) {
			Map<String, Object> lessonDataAsMap = new HashMap<String, Object>();
			List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
			long scoreMet = 0; long scoreNotMet = 0; long assessmentAttempted = 0; long assessmentNotAttempted = 0; 
			long collectionNotViewed = 0; long collectionViewed = 0; long assessmentCount = 0; 
			String lessonGooruOid = lesson.getName();
			String classLessonKey = getBaseService().appendTilda(classId, courseId, unitId, lessonGooruOid);
			if (StringUtils.isNotBlank(userUid)) {
				classLessonKey = getBaseService().appendTilda(classLessonKey, userUid);
			}
			OperationResult<ColumnList<String>> lessonMetaData = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), lessonGooruOid, resourceColumns);
			lessonDataAsMap.put(ApiConstants.GOORUOID, lessonGooruOid);
			lessonDataAsMap.put(ApiConstants.SEQUENCE, lesson.getLongValue());
			lessonDataAsMap.put(ApiConstants.TITLE, null);
			if (!lessonMetaData.getResult().isEmpty() && lessonMetaData.getResult().size() > 0) {
				ColumnList<String> lessonMetaDataColumns = lessonMetaData.getResult();
				lessonDataAsMap.put(ApiConstants.TITLE, lessonMetaDataColumns.getColumnByName(ApiConstants.TITLE).getStringValue());
			}
			// fetch item progress data
			OperationResult<ColumnList<String>> itemData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), lessonGooruOid);
			ColumnList<String> items = itemData.getResult();
			
			ColumnList<String> lessonMetricColumnList = null;
			OperationResult<ColumnList<String>> lessonMetricsData = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), classLessonKey);
			if (lessonMetricsData != null && !lessonMetricsData.getResult().isEmpty()) {
				lessonMetricColumnList = lessonMetricsData.getResult();
			}
			for (Column<String> item : items) {
				Map<String, Object> itemDataAsMap = new HashMap<String, Object>();
				String itemGooruOid = item.getName();
				itemDataAsMap.put(ApiConstants.GOORUOID, itemGooruOid);
				itemDataAsMap.put(ApiConstants.SEQUENCE, item.getLongValue());

				// fetch item usage data
				long itemViews = 0l;
				Long assessmentScore = null;
				if(lessonMetricColumnList != null && lessonMetricColumnList.size() > 0) {
					itemViews = lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid,ApiConstants.VIEWS), 0L);
					assessmentScore = lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid,ApiConstants._SCORE_IN_PERCENTAGE), null);
					itemDataAsMap.put(ApiConstants.VIEWS, itemViews);
					itemDataAsMap.put(ApiConstants.TIMESPENT, lessonMetricColumnList.getLongValue(getBaseService().appendTilda(itemGooruOid, ApiConstants._TIME_SPENT), 0L));
				}
				
				// fetch type
				getResourceMeta(itemDataAsMap, isSecure, traceId, itemGooruOid, getBaseService().convertStringToCollection(ApiConstants.RESOURCE_TYPE));
				itemDataMapAsList.add(itemDataAsMap);
				
				String itemType = null;
				if (itemDataAsMap.get(ApiConstants.TYPE) != null && StringUtils.isNotBlank(itemDataAsMap.get(ApiConstants.TYPE).toString())) {
					itemType = itemDataAsMap.get(ApiConstants.TYPE).toString();
				}
				if (itemType != null) {
                    if (itemType.matches(ApiConstants.ASSESSMENT_TYPES)) {
						// fetch lesson's score status data
						assessmentCount++;
						if (assessmentScore != null && assessmentScore >= classMinScore) {
							scoreMet++;
							assessmentAttempted++;
						} else if (assessmentScore != null && assessmentScore < classMinScore) {
							scoreNotMet++;
							assessmentAttempted++;
						}
						if (assessmentScore == null) {
							assessmentNotAttempted++;
						}
					} else if (itemType.equalsIgnoreCase(ApiConstants.COLLECTION)) {
						if(itemViews > 0) {
							collectionViewed++;
						} else {
							collectionNotViewed++;
						}
					}
				}
				itemDataAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, assessmentScore != null ? assessmentScore : 0L);
			}
			
			lessonDataAsMap.putAll(getActivityMetricsAsMap(traceId, classLessonKey, lessonGooruOid));
			lessonDataAsMap.put(ApiConstants.TYPE, ApiConstants.LESSON);
			lessonDataAsMap.put(ApiConstants.ITEM, itemDataMapAsList);
			String lessonScoreStatus = null;
			if (assessmentAttempted == 0 && (itemDataMapAsList.size() == assessmentNotAttempted)) {
				lessonScoreStatus = ApiConstants.NOT_ATTEMPTED;
			} else if(scoreNotMet > 0 || (assessmentAttempted > 0 && (assessmentCount != 0L && scoreMet < assessmentCount))) { 
				lessonScoreStatus = ApiConstants.SCORE_NOT_MET;
			} else if (scoreMet > 0 && scoreNotMet == 0 && assessmentNotAttempted == 0 && (assessmentCount != 0L && scoreMet == assessmentCount)) {
				lessonScoreStatus = ApiConstants.SCORE_MET;
			} else if(assessmentNotAttempted > 0) {
				lessonScoreStatus = ApiConstants.NOT_ATTEMPTED;
			} else if(collectionViewed == items.size() && collectionNotViewed == 0) {
				lessonScoreStatus = ApiConstants.VIEWED;
			} else if((collectionNotViewed == items.size() && collectionViewed == 0) || collectionNotViewed > 0){
				lessonScoreStatus = ApiConstants.NOT_VIEWED;
			} 
			
			lessonDataAsMap.put(ApiConstants.SCORE_STATUS, lessonScoreStatus);
			if (!lessonDataAsMap.isEmpty() && lessonDataAsMap.size() > 0) {
				resultDataMapAsList.add(lessonDataAsMap);
			}
		}
		}
		responseParamDTO.setContent(resultDataMapAsList);
		return responseParamDTO;
	}
	
	public ResponseParamDTO<Map<String,Object>> getLessonAssessmentsUsage(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentIds, String userUid, boolean isSecure) throws Exception {
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
		Long classMinScore = 0L; String sessionKey = null;
		Long scoreInPercentage = 0L; Long score = 0L; String evidence = null; Long timespent = 0L; Long scorableCountOnEvent = 0L; Long totalReaction = 0L; Long reactedCount = 0L; Long avgReaction = 0L;
		
		if ((sessionId != null && StringUtils.isNotBlank(sessionId.trim()))) {
			sessionKey = sessionId;
		} else if ((classId != null && StringUtils.isNotBlank(classId.trim())) && (courseId != null && StringUtils.isNotBlank(courseId.trim())) 
				&& (unitId != null && StringUtils.isNotBlank(unitId.trim())) && (lessonId != null && StringUtils.isNotBlank(lessonId.trim()))
				&& (userUid != null && StringUtils.isNotBlank(userUid.trim()))) {
			sessionKey = getSessionIdFromKey(traceId, getBaseService().appendTilda(SessionAttributes.RS.getSession(), classId, courseId, unitId, lessonId, assessmentId, userUid));
		} else if((userUid != null && StringUtils.isNotBlank(userUid.trim()))) {
			sessionKey = getSessionIdFromKey(traceId, getBaseService().appendTilda(SessionAttributes.RS.getSession(), assessmentId, userUid));
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E111, getBaseService().appendComma("contentGooruId", "sessionId")
					, getBaseService().appendComma("contentGooruId", "userUid"));
		}
		
		// Fetch goal for the class
		if (classId != null && StringUtils.isNotBlank(classId.trim())) {
			OperationResult<ColumnList<String>> classData = getCassandraService().read(traceId, ColumnFamily.CLASS.getColumnFamily(), classId);
			if (classData != null && !classData.getResult().isEmpty() && classData.getResult().size() > 0) {
				classMinScore = classData.getResult().getLongValue(ApiConstants.MINIMUM_SCORE, 0L);
			}
		}
		
		//Fetch score and evidence of assessment
		if (StringUtils.isNotBlank(sessionKey)) {
			itemsColumnList = getCassandraService().read(traceId, ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionKey);
			if (itemsColumnList != null && !itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
				ColumnList<String> lessonMetricColumns = itemsColumnList.getResult();
				score = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants.SCORE), 0L);
				scoreInPercentage = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._SCORE_IN_PERCENTAGE), 0L);
				evidence = lessonMetricColumns.getStringValue(getBaseService().appendTilda(assessmentId, ApiConstants.EVIDENCE), null);
				timespent = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._TIME_SPENT), 0L);
				scorableCountOnEvent = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._QUESTION_COUNT), 0L);
				totalReaction = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._TOTAL_REACTION), 0L);
				reactedCount = lessonMetricColumns.getLongValue(getBaseService().appendTilda(assessmentId, ApiConstants._REACTED_COUNT), 0L);
				if(reactedCount != 0L) {
					avgReaction = totalReaction/reactedCount;
				}
			} else {
				logger.info("No session available for key" + sessionKey);
			}
		}
		
		//Fetch assessment metadata
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ApiConstants.TITLE);
		resourceColumns.add(ApiConstants.RESOURCE_TYPE);
		resourceColumns.add(ApiConstants.THUMBNAIL);
		resourceColumns.add(ApiConstants.GOORUOID);
		resourceColumns.add(ApiConstants.FOLDER);
		getResourceMeta(itemDetailAsMap, isSecure, traceId, assessmentId, resourceColumns);

		
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
		itemDetailAsMap.put(ApiConstants.SCORABLE_COUNT_ON_EVENT, scorableCountOnEvent);
		itemDetailAsMap.put(ApiConstants.SESSIONID, sessionKey);
		itemDetailAsMap.put(ApiConstants.AVG_REACTION, avgReaction);
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
		itemDetailAsMap.put(ApiConstants.PROFILE_URL, filePath.getProperty(ApiConstants.USER_PROFILE_URL_PATH).replaceAll(ApiConstants.ID, userUid));
		itemDetailAsMap.put(ApiConstants.GOAL, classMinScore);
		itemDetailAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, scoreInPercentage);
		itemDetailAsMap.put(ApiConstants.EVIDENCE, evidence);
		itemDetailAsMap.put(ApiConstants.SCORE, score);
		itemDetailAsMap.put(ApiConstants.TIMESPENT, timespent);
		itemDetailAsMap.put(ApiConstants.SESSION, sessionResponse.getContent());
		itemDetailAsMap.put(ApiConstants.AVG_REACTION, avgReaction);
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
	private String getSessionIdFromKey(String traceId, String recentSessionKey) {
		String recentSessionId = null;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.SESSION.getColumnFamily(), getBaseService().appendTilda(recentSessionKey));
		if(!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			recentSessionId = itemsColumnList.getResult().getStringValue(ApiConstants._SESSION_ID, null);
		}
		return recentSessionId;
	}
	
	private List<Map<String, Object>> getResourceData(String traceId, boolean isSecure, String keys, Collection<String> columnsToFetch, String type) {
		List<Map<String, Object>> rawDataMapAsList = getBaseService().getRowsColumnValues(traceId, getCassandraService().readAll(traceId, ColumnFamily.RESOURCE.getColumnFamily(), null, keys, new String(), columnsToFetch));
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

	private void getResourceMeta(Map<String, Object> dataMap, boolean isSecure, String traceId, String key, Collection<String> columnsToFetch) {
		//Set default
		for (String column : columnsToFetch) {
			if (column.equalsIgnoreCase("question.type") || column.equalsIgnoreCase("question.questionType") || column.equalsIgnoreCase("resourceType")) {
				dataMap.put(ApiConstants.TYPE, null);
			} else if(column.equalsIgnoreCase(ApiConstants.FOLDER)) { 
				continue;
			} else{
				dataMap.put(column, null);
			}
		}

		// fetch metadata
		ColumnList<String> resourceColumn = getCassandraService().read(traceId, ColumnFamily.RESOURCE.getColumnFamily(), key, columnsToFetch).getResult();
		// form thumbnail
		String thumbnail = resourceColumn.getStringValue(ApiConstants.THUMBNAIL, null);
		if (StringUtils.isNotBlank(thumbnail)) {
			String nfsPath = filePath.getProperty(ApiConstants.NFS_BUCKET);
			String folder = resourceColumn.getStringValue(ApiConstants.FOLDER, null);
			String thumb = null;
			if (thumbnail.startsWith(ApiConstants._HTTP)) {
				thumb = thumbnail;
			} else {
				if (thumbnail.contains(folder)) {
					thumb = getBaseService().appendForwardSlash(nfsPath, thumbnail);
				} else {
					thumb = getBaseService().appendForwardSlash(nfsPath, folder, thumbnail);
				}
			}
			if (isSecure) {
				thumb = thumb.replaceFirst(ApiConstants._HTTP, ApiConstants._HTTPS);
			}
			dataMap.put(ApiConstants.THUMBNAIL, thumb);
		}

		for (Column<String> column : resourceColumn) {
			if (column.getName().equals(ApiConstants.RESOURCE_TYPE) || column.getName().equalsIgnoreCase("question.type") || column.getName().equalsIgnoreCase("question.questionType")) {
				dataMap.put(ApiConstants.TYPE, column.getStringValue());
			} else if (column.getName().equals(ApiConstants.THUMBNAIL) || column.getName().equals(ApiConstants.FOLDER)) {
				continue;
			} else {
				dataMap.put(column.getName(), column.getStringValue());
			}
		}
	}
	
	public List<Map<String,Object>> getResourcesMetaData(String traceId, Collection<String> keys,Collection<String> resourceColumns,String type,Map<String,String> aliesNames) {

		OperationResult<Rows<String, String>> resourceRows = getCassandraService().readAll(traceId, ColumnFamily.RESOURCE.getColumnFamily(), keys, resourceColumns);
		List<Map<String,Object>> resourceMetaList = new ArrayList<Map<String,Object>>();
		for(Row<String,String> row : resourceRows.getResult()){
			Map<String,Object> resourceMetaData = new HashMap<String,Object>();
			if(type != null){
				String resourceType = row.getColumns().getStringValue(ApiConstants.RESOURCE_TYPE, ApiConstants.STRING_EMPTY);
				if(!resourceType.matches(type)){
						continue;
				}
			}
			Map<String,List<String>> mergeResourceDualColumnValues = DataUtils.getMergeDualColumnValues().get(ColumnFamily.RESOURCE.getColumnFamily());
			resourceMetaData = DataUtils.getColumnFamilyContent(traceId, ColumnFamily.RESOURCE.getColumnFamily(), row.getColumns(), aliesNames, resourceColumns, mergeResourceDualColumnValues);
			resourceMetaList.add(resourceMetaData);
		}
		return resourceMetaList;
	}
	
	private Map<String, Object> getClassMetricsAsMap(String traceId, String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		Long views = 0L; Long timeSpent = 0L; Long score = 0L; 
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			views = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants.VIEWS), 0L);
			timeSpent = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants._TIME_SPENT), 0L);
			score = itemMetricColumns.getLongValue(getBaseService().appendTilda(contentGooruOid, ApiConstants._SCORE_IN_PERCENTAGE), 0L);
		}
		usageAsMap.put(ApiConstants.VIEWS, views);
		usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
		usageAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, score);
		return usageAsMap;
	}
	
	public void getResourceMetaData(Map<String, Object> dataMap, String traceId,String type, String key,Map<String,String> aliesNames) {
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
		Map<String,List<String>> mergeResourceDualColumnValues = DataUtils.getMergeDualColumnValues().get(ColumnFamily.RESOURCE.getColumnFamily());
		dataMap = DataUtils.getColumnFamilyContent(traceId, ColumnFamily.RESOURCE.getColumnFamily(), resourceColumn, aliesNames, resourceColumns, mergeResourceDualColumnValues);
	}
	
	private List<Map<String, Object>> getClassMetricsForAllItemsAsMap(String traceId, String key, String contentGooruOids) {
		List<Map<String, Object>> usageAsMapAsList = new ArrayList<Map<String, Object>>();;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if(StringUtils.isNotBlank(contentGooruOids)) {
			for (String itemGooruOid : contentGooruOids.split(ApiConstants.COMMA)) {
				Map<String, Object> usageAsMap = new HashMap<String, Object>();
				usageAsMap.put(ApiConstants.GOORUOID, itemGooruOid);
				Long views = 0L; Long timeSpent = 0L; Long score = 0L; String collectionType = null;Long lastAccessed = null;String evidence = null;
				if (itemsColumnList != null && !itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
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
		}
		return usageAsMapAsList;
	}
	
	private Map<String, Object> getActivityMetricsAsMap(String traceId, String key, String contentGooruOid) {
		Map<String, Object> usageAsMap = new HashMap<String, Object>();
		usageAsMap.put(ApiConstants.GOORUOID, contentGooruOid);
		Long views = 0L; Long timeSpent = 0L; Long score = 0L;
		OperationResult<ColumnList<String>> itemsColumnList = getCassandraService().read(traceId, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		if (!itemsColumnList.getResult().isEmpty() && itemsColumnList.getResult().size() > 0) {
			ColumnList<String> itemMetricColumns = itemsColumnList.getResult();
			views = itemMetricColumns.getLongValue(ApiConstants.VIEWS, 0L);
			timeSpent = itemMetricColumns.getLongValue(ApiConstants._TIME_SPENT, 0L);
			score = itemMetricColumns.getLongValue(ApiConstants._SCORE_IN_PERCENTAGE, 0L);
		}
		usageAsMap.put(ApiConstants.VIEWS, views);
		usageAsMap.put(ApiConstants.SCORE_IN_PERCENTAGE, score);
		usageAsMap.put(ApiConstants.TIMESPENT, timeSpent);
		return usageAsMap;
	}
	
	public List<Map<String,Object>> getDirectActivityMetrics(String traceId, Collection<String> rowKeys,String columnFamily, Collection<String> requestedColumns, String studentIds,boolean isUserIdInKey,String contentIds, boolean userProcess) {
		Collection<String> fetchedContentIds = new ArrayList<String>();
		List<Map<String,Object>> contentUsageData = new ArrayList<Map<String,Object>>();
		Map<String,Set<String>> studentContentMapper = new HashMap<String,Set<String>>();

		/**
		 * Get Activity data
		 */
		OperationResult<Rows<String, String>> activityData = getCassandraService().readAll(traceId, columnFamily, rowKeys, requestedColumns);
		if (!activityData.getResult().isEmpty()) {
			
			Rows<String, String> itemMetricRows = activityData.getResult();
			//Iterate for Every Row
			for(Row<String, String> metricRow : itemMetricRows){
				String userId = null;
				String contentId = null;
				Map<String,Object> usageMap = new HashMap<String,Object>();
				//Get content Id
				for(String id : contentIds.split(ApiConstants.COMMA)){
					if(metricRow.getKey().contains(id)){
						contentId = id;
						break;
					}
				}
				usageMap.put(ApiConstants.GOORUOID, contentId);
				//Iterate for Fetched column
				for(String column : requestedColumns){
					//Get the metric data
					if(column.equalsIgnoreCase(ApiConstants._SCORE_IN_PERCENTAGE)){
						usageMap.put(ApiConstants.SCORE_IN_PERCENTAGE, metricRow.getColumns().getLongValue(ApiConstants._SCORE_IN_PERCENTAGE, 0L));
					} else if(column.equalsIgnoreCase(ApiConstants._ASSESSMENT_UNIQUE_VIEWS)){
						usageMap.put(ApiConstants.VIEWS, metricRow.getColumns().getLongValue(ApiConstants._ASSESSMENT_UNIQUE_VIEWS, 0L));
					}
				}
				//Get the userId for the content usage,If we need user level tril down
				if(userProcess){
					userId = includeUserId(userProcess,isUserIdInKey,studentIds,userId,contentId,metricRow,usageMap,studentContentMapper);
				}
				//Storing the Fetched content id to support including default value at content level 
				if(!fetchedContentIds.contains(contentId) && contentId.length() > 35){
					fetchedContentIds.add(contentId);
				}
				contentUsageData.add(usageMap);
			}
		}
		/**
		 * Set default value at user-content level
		 */
		if(userProcess){
			insertDefaultUserContents(contentIds, studentContentMapper, studentIds, requestedColumns,contentUsageData);
		}else {
			/**
			 * Set default value only at collection level
			 */
			for(String id : contentIds.split(ApiConstants.COMMA)){
				if(!fetchedContentIds.contains(id)){
					Map<String,Object> tempMap = insertDefaultMetrics(requestedColumns);
					tempMap.put(ApiConstants.GOORUOID, id);
					contentUsageData.add(tempMap);
				}
			}
		}
		return contentUsageData;
	}
	
	public List<Map<String,Object>> getIdSeparatedMetrics(String traceId, Collection<String> rowKeys,String columnFamily, Collection<String> requestedColumns, String studentIds,boolean isUserIdInKey,String contentIds, boolean userProcess) {

		Collection<String> fetchedContentIds = new ArrayList<String>();
		List<Map<String,Object>> contentUsageData = new ArrayList<Map<String,Object>>();
		Map<String,Set<String>> studentContentMapper = new HashMap<String,Set<String>>();

		/**
		 * Get Activity data
		 */
		OperationResult<Rows<String, String>> activityData = getCassandraService().readAll(traceId, columnFamily, rowKeys, requestedColumns);
		if (!activityData.getResult().isEmpty()) {
			
			Rows<String, String> itemMetricRows = activityData.getResult();
			//Iterate for Every Row
			for(Row<String, String> metricRow : itemMetricRows){
				String userId = null;
				Map<String,Map<String, Object>> idBasedContentUsage = new HashMap<String,Map<String, Object>>();
				//Iterate for Fetched column
				for(String column : requestedColumns){
					Map<String,Object> usageMap = new HashMap<String,Object>();
					String[] columnMetaInfo = column.split(ApiConstants.TILDA);
					String metricName = (columnMetaInfo.length > 1) ? columnMetaInfo[columnMetaInfo.length-1] : columnMetaInfo[0];
					//No Need to process for MA question A,B,C,D option
					if((columnMetaInfo.length > 1 ? columnMetaInfo[1].matches(ApiConstants.OPTIONS_MATCH) : columnMetaInfo[0].matches(ApiConstants.OPTIONS_MATCH))){
						continue;
					}
					//Get the metric data
					usageMap = fetchMetricData(traceId,columnMetaInfo[0],metricRow,metricName,column);
					//Get the userId for the content usage,If we need user level tril down
					if(userProcess){
						userId = includeUserId(userProcess,isUserIdInKey,studentIds,userId,columnMetaInfo[0],metricRow,usageMap,studentContentMapper);
					}
					//Since content are stored in column,we need to do a content based separation
					if(idBasedContentUsage.containsKey(columnMetaInfo[0])){
						usageMap.putAll(idBasedContentUsage.get(columnMetaInfo[0]));
					}
					idBasedContentUsage.put(columnMetaInfo[0], usageMap);
					//Storing the Fetched content id to support including default value at content level 
					if(!fetchedContentIds.contains(columnMetaInfo[0]) && columnMetaInfo[0].length() > 35){
						fetchedContentIds.add(columnMetaInfo[0]);
					}
				}
				contentUsageData.addAll(getBaseService().convertMapToList(idBasedContentUsage, ApiConstants.GOORUOID));
			}
		}
		
		/**
		 * Set default value at user-content level
		 */
		if(userProcess){
			insertDefaultUserContents(contentIds, studentContentMapper, studentIds, requestedColumns,contentUsageData);
		}else {
			/**
			 * Set default value only at collection level
			 */
			for(String id : contentIds.split(ApiConstants.COMMA)){
				if(!fetchedContentIds.contains(id)){
					Map<String,Object> tempMap = insertDefaultMetrics(requestedColumns);
					tempMap.put(ApiConstants.GOORUOID, id);
					contentUsageData.add(tempMap);
				}
			}
		}
		return contentUsageData;
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
			userMap.put(ApiConstants.USER_UID, column.getName());
			userMap.put(ApiConstants.USER_NAME, column.getStringValue());
			userList.add(userMap);
		}
		return userList;
	} 
	
	public List<Map<String,Object>> getContentItems(String traceId,String rowKey,String type,boolean fetchMetaData, Collection<String> columnNames, Map<String,String> aliesName){
		OperationResult<ColumnList<String>> assessmentData = getCassandraService().read(traceId, ColumnFamily.COLLECTION_ITEM_ASSOC.getColumnFamily(), rowKey);
		List<Map<String,Object>> contentItems = new ArrayList<Map<String,Object>>();
		List<String> resourceIds = new ArrayList<String>();
		for(Column<String> column : assessmentData.getResult()){
			Map<String, Object> dataMap = new HashMap<String,Object>();
			dataMap.put(ApiConstants.SEQUENCE, column.getLongValue());
			dataMap.put(ApiConstants.GOORUOID, column.getName());
			resourceIds.add(column.getName());
			contentItems.add(dataMap);
		}
		if(fetchMetaData){
			if(columnNames == null || columnNames.isEmpty()){
				columnNames = new ArrayList<String>();
				columnNames.add(ApiConstants.TITLE);
				columnNames.add(ApiConstants.GOORUOID);
				columnNames.add(ApiConstants._GOORUOID);
				columnNames.add(ApiConstants.RESOURCE_TYPE);
			}
			List<Map<String,Object>> resourceMetaData = getResourcesMetaData(traceId,resourceIds,columnNames,type,aliesName);
			contentItems = getBaseService().innerJoin(resourceMetaData, contentItems, ApiConstants.GOORUOID);
		}                 
		return contentItems;
	}

	private ResponseParamDTO<Map<String, Object>> getAllStudentProgressByUnit(String traceId, String classId, String courseId, boolean isSecure) throws Exception {

		List<Map<String, Object>> contentUsage = new ArrayList<Map<String, Object>>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();

		List<Map<String,Object>> unitsMetaData = getContentItems(traceId,courseId,null,true, null, DataUtils.getResourceFields());
		List<Map<String,Object>> students = getStudents(traceId, classId);
		if(!unitsMetaData.isEmpty() && !students.isEmpty()){
		String classCourseId = getBaseService().appendTilda(classId,courseId);
		StringBuffer unitIds = getBaseService().getCommaSeparatedIds(unitsMetaData, ApiConstants.GOORUOID);
		StringBuffer studentIds = getBaseService().getCommaSeparatedIds(students, ApiConstants.USER_UID);
		Collection<String> UnitStudentKeys = getBaseService().generateCommaSeparatedStringToKeys(ApiConstants.TILDA, unitIds.toString(),studentIds.toString());
		Collection<String> keys = new ArrayList<String>();
		for(String unitStudentKey : UnitStudentKeys){
			keys.add(getBaseService().appendTilda(classCourseId,unitStudentKey));
		}
		UnitStudentKeys.clear();
		UnitStudentKeys.add(ApiConstants._SCORE_IN_PERCENTAGE);
		UnitStudentKeys.add(ApiConstants._ASSESSMENT_UNIQUE_VIEWS);
		contentUsage = getDirectActivityMetrics(traceId, keys,ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), UnitStudentKeys, studentIds.toString(),true,unitIds.toString(),true);
		contentUsage = getBaseService().leftJoin(contentUsage,unitsMetaData,ApiConstants.GOORUOID,ApiConstants.GOORUOID);
		//group at content level
		contentUsage = getBaseService().groupRecordsBasedOnKey(contentUsage,ApiConstants.USER_UID,ApiConstants.USAGE_DATA);
		contentUsage = getBaseService().leftJoin(contentUsage, students, ApiConstants.USER_UID, ApiConstants.USER_UID);
		}
		responseParamDTO.setContent(contentUsage);
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
		//Get list of resources and students
		Map<String, String> resourceFields = DataUtils.getResourceFields();
		resourceFields.put(ApiConstants.QUESTION_DOT_TYPE, ApiConstants.QUESTION_TYPE);
		resourceFields.put(ApiConstants.QUESTION_DOT_QUESTION_TYPE, ApiConstants.QUESTION_TYPE);
		List<Map<String,Object>> resourcesMetaData = getContentItems(traceId,collectionId,null,true,resourceFields.keySet(),resourceFields);
		List<Map<String,Object>> studentsMetaData = getStudents(traceId, classId);
		
		StringBuffer resourceIds = getBaseService().getCommaSeparatedIds(resourcesMetaData, ApiConstants.GOORUOID);
		StringBuffer studentIds = getBaseService().getCommaSeparatedIds(studentsMetaData, ApiConstants.USER_UID);
		Set<String> columnSuffix =  DataUtils.getStudentsCollectionUsageColumnSuffix();
		//Fetch session data
		Collection<String> rowKeys = getBaseService().generateCommaSeparatedStringToKeys(ApiConstants.TILDA, getBaseService().appendTilda(SessionAttributes.RS.getSession(),classId,courseId,unitId,lessonId,collectionId), studentIds.toString());
		List<String> sessionIds = getSessions(traceId,rowKeys);
		//Fetch collection actiivity data
		Collection<String> columns = getBaseService().generateCommaSeparatedStringToKeys(ApiConstants.TILDA, resourceIds.toString(), columnSuffix);
		columns.add(ApiConstants.GOORU_UID);
		List<Map<String,Object>> assessmentUsage = getIdSeparatedMetrics(traceId, sessionIds,ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), columns, studentIds.toString(),false,resourceIds.toString(),true);
		assessmentUsage = getBaseService().leftJoin(assessmentUsage, studentsMetaData, ApiConstants.USER_UID, ApiConstants.USER_UID);
		//Group data at user level
		assessmentUsage = getBaseService().groupRecordsBasedOnKey(assessmentUsage,ApiConstants.GOORUOID,ApiConstants.USAGE_DATA);
		assessmentUsage = getBaseService().leftJoin(resourcesMetaData,assessmentUsage,ApiConstants.GOORUOID,ApiConstants.GOORUOID);
		//Setting assessment meta data
        List<Map<String,Object>> assessmentMetaInfo = getQuestionMetaData(traceId,collectionId);
        assessmentUsage = getBaseService().leftJoin(assessmentUsage, assessmentMetaInfo, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
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
		OperationResult<Rows<String, String>> questionMetaDatas = getCassandraService().readAll(traceId, ColumnFamily.ASSESSMENT_ANSWER.getColumnFamily(), ApiConstants.COLLECTIONGOORUOID, collectonId, columns);
		Map<String,Object> resultMap = new HashMap<String,Object>();
		for(Row<String, String> row : questionMetaDatas.getResult()){
			Map<String,Object> dataMap = new HashMap<String,Object>();
			String key = row.getColumns().getStringValue(ApiConstants.QUESTION_GOORU_OID, null);
			dataMap.put(ApiConstants._QUESTIONGOORUOID, row.getColumns().getStringValue(ApiConstants.QUESTION_GOORU_OID, null));
			dataMap.put(ApiConstants.QUESTION_TYPE, row.getColumns().getStringValue(ApiConstants._QUESTION_TYPE, null));
			dataMap.put(ApiConstants.SEQUENCE, row.getColumns().getIntegerValue(ApiConstants.SEQUENCE, 0));
			dataMap.put(ApiConstants.IS_CORRECT, row.getColumns().getIntegerValue(ApiConstants._IS_CORRECT, 0));
			dataMap.put(ApiConstants.ANSWER_ID, row.getColumns().getLongValue(ApiConstants._ANSWER_ID, 0L));
			dataMap.put(ApiConstants._ANSWERTEXT, row.getColumns().getStringValue(ApiConstants.ANSWER_TEXT, null));
			dataMap.put(ApiConstants.QUESTION_ID, row.getColumns().getLongValue(ApiConstants._QUESTION_ID, 0L));
			dataMap.put(ApiConstants.TYPE, row.getColumns().getStringValue(ApiConstants._TYPE_NAME, null));
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

	private Map<String, Object> insertDefaultMetrics(Collection<String> columns) {
		Map<String, Object> usageMap = new HashMap<String, Object>();
		for (String metricName : columns) {
			if (metricName.endsWith(ApiConstants._COLLECTION_TYPE)) {
				usageMap.put(ApiConstants.COLLECTION_TYPE, null);
			} else if (metricName.endsWith(ApiConstants.VIEWS)) {
				usageMap.put(ApiConstants.VIEWS, 0L);
			} else if (metricName.endsWith(ApiConstants._SCORE_IN_PERCENTAGE)) {
				usageMap.put(ApiConstants.SCORE_IN_PERCENTAGE, 0L);
			} else if (metricName.endsWith(ApiConstants._ANSWER_OBJECT)) {
				usageMap.put(ApiConstants.ANSWER_OBJECT, null);
			}else if(metricName.endsWith(ApiConstants.ATTEMPTS)){
				usageMap.put(ApiConstants.ATTEMPTS, 0L);
			}else if(metricName.endsWith(ApiConstants.SCORE)){
				usageMap.put(ApiConstants.SCORE, 0L);
			}else if(metricName.endsWith(ApiConstants._AVG_TIME_SPENT)) {
				usageMap.put(ApiConstants.AVG_TIME_SPENT, 0L);
			}else if(metricName.endsWith(ApiConstants.CHOICE)) {
				usageMap.put(ApiConstants.TEXT, null);
			}else if(metricName.endsWith(ApiConstants._AVG_REACTION)){
				usageMap.put(ApiConstants.AVG_REACTION, 0L);
			}else if(metricName.endsWith(ApiConstants.REACTION)){
				usageMap.put(ApiConstants.REACTION, 0L);
			}else if(metricName.endsWith(ApiConstants._QUESTION_STATUS)){
				usageMap.put(ApiConstants.STATUS, null);
			}else if(metricName.endsWith(ApiConstants._TIME_SPENT)) {
				usageMap.put(ApiConstants.TIMESPENT, 0L);
			} else if(metricName.endsWith(ApiConstants.OPTIONS)) {
				usageMap.put(ApiConstants.OPTIONS, null);
			}
		}
		return usageMap;
	}
	
	private Map<String,Object> fetchMetricData(String traceId, String id,Row<String, String> metricRow, String metricName,String column){
		Map<String,Object> usageMap = new HashMap<String,Object>();
 		if(metricName.equals(ApiConstants._COLLECTION_TYPE)){
			usageMap.put(ApiConstants.COLLECTION_TYPE, metricRow.getColumns().getStringValue(column, null));
		}else if(metricName.equals(ApiConstants.VIEWS)){
			usageMap.put(ApiConstants.VIEWS, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants._SCORE_IN_PERCENTAGE)){
			usageMap.put(ApiConstants.SCORE_IN_PERCENTAGE, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants._TIME_SPENT)){
			usageMap.put(ApiConstants.TIMESPENT, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if (metricName.equals(ApiConstants._AVG_TIME_SPENT)) {
			usageMap.put(ApiConstants.AVG_TIME_SPENT, metricRow.getColumns().getLongValue(column.trim(), 0L));
		} else if(metricName.equals(ApiConstants._ANSWER_OBJECT)){
			usageMap.put(ApiConstants.ANSWER_OBJECT, metricRow.getColumns().getStringValue(column.trim(), null));
		}else if(metricName.equals(ApiConstants.ATTEMPTS)){
			usageMap.put(ApiConstants.ATTEMPTS, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants.CHOICE)){
			usageMap.put(ApiConstants.TEXT, metricRow.getColumns().getStringValue(column.trim(), null));
		}else if(metricName.equals(ApiConstants.REACTION)){
			usageMap.put(ApiConstants.REACTION, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants._AVG_REACTION)){
			usageMap.put(ApiConstants.AVG_REACTION, metricRow.getColumns().getLongValue(column.trim(), 0L));
		}else if(metricName.equals(ApiConstants._QUESTION_STATUS)){
			String responseStatus = metricRow.getColumns().getStringValue(column.trim(), null);
			usageMap.put(ApiConstants.STATUS, responseStatus);
			usageMap.put(ApiConstants.SCORE, (responseStatus != null && responseStatus.equalsIgnoreCase(ApiConstants.CORRECT)) ? 1L : 0L);
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
			usageMap.put(ApiConstants.USER_UID, metricRow.getColumns().getStringValue(ApiConstants.GOORU_UID, null));
		}else {
			try{
				usageMap.put(metricName, metricRow.getColumns().getStringValue(column, null));
			}catch(Exception e){
				InsightsLogger.error(traceId, getBaseService().errorHandler(ErrorMessages.UNHANDLED_EXCEPTION, ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), column),e);
			}
		}
 		return usageMap;
	}
	
	private void insertDefaultUserContents(String collectionIds, Map<String,Set<String>> userSet, String userIds,Collection<String> columns, List<Map<String,Object>> collectionUsageData){
	
		for(String collectionId : collectionIds.split(ApiConstants.COMMA)){
			
			Map<String,Object> tempMap = insertDefaultMetrics(columns);
			tempMap.put(ApiConstants.GOORUOID, collectionId);
			if(userSet.containsKey((collectionId))){
				insertDataToDefaultUsers(userSet,collectionId,userIds,tempMap,collectionUsageData);
			}else{
				userSet.put(collectionId, new HashSet<String>());
				insertDataToDefaultUsers(userSet,collectionId,userIds,tempMap,collectionUsageData);
			}
		}
	}
	
	private void insertDataToDefaultUsers(Map<String,Set<String>> userSet,String collectionId,String userIds,Map<String,Object> tempMap, List<Map<String,Object>> collectionUsageData){
		Set<String> temp = userSet.get(collectionId);
		for(String user : userIds.split(ApiConstants.COMMA)){
			Map<String,Object> userData = new HashMap<String,Object>(tempMap);
			if(!temp.contains(user)){
				userData.put(ApiConstants.USER_UID, user);
				collectionUsageData.add(userData);
			}
		}
	}
	private String includeUserId(boolean userProcess,boolean isUserIdInKey,String userIds,String userId,String contentId,Row<String,String> metricRow,Map<String,Object> usageMap,Map<String,Set<String>> studentContentMapper){
		
		/**
		 * Assign the User Id, Check is the user exists in key then iterate the userIds
		 *  or else fetch from column
		 */
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
		usageMap.put(ApiConstants.USER_UID, userId);

		/**
		 * Store the User Ids to every content,so that it helps to include default value.
		 */
		if(userProcess){
		Set<String>	setHandler = new HashSet<String>();
		setHandler.add(userId);
			if(studentContentMapper.containsKey(contentId)){
				setHandler.addAll(studentContentMapper.get(contentId));
			}
		studentContentMapper.put(contentId, setHandler);
		}
		return userId;
	}

	public ResponseParamDTO<Map<String, Object>> getStudentAssessmentSummary(String traceId, String classId, String courseId, String unitId, String lessonId, String assessmentId, String userUid,
			String sessionId, boolean isSecure) throws Exception {

		List<Map<String, Object>> itemDataMapAsList = new ArrayList<Map<String, Object>>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		
		//Fetch sessionId from recent session if sessionId is not requested in call
		String recentSessionKey = null;
		if ((sessionId != null && StringUtils.isNotBlank(sessionId.trim()))) {
			recentSessionKey = sessionId;
		} else if (StringUtils.isNotBlank(classId) && StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(unitId) 
				&&  StringUtils.isNotBlank(lessonId) && StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(assessmentId)) {
			recentSessionKey = getSessionIdFromKey(traceId, getBaseService().appendTilda(SessionAttributes.RS.getSession(), classId, courseId, unitId, lessonId, assessmentId, userUid));
		} else if(StringUtils.isNotBlank(userUid) && StringUtils.isNotBlank(assessmentId)) {
			recentSessionKey = getSessionIdFromKey(traceId, getBaseService().appendTilda(SessionAttributes.RS.getSession(), assessmentId, userUid));
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorCodes.E112,  
					getBaseService().appendComma("contentGooruId", "sessionId"),
					getBaseService().appendComma("contentGooruId", "sessionId", "classGooruId", "courseGooruId", "unitGooruId", "lessonGooruId"),
					getBaseService().appendComma("contentGooruId", "userUid"));
		}
		
		//Fetch collection summary data
		if (recentSessionKey != null) {
			logger.info("Fetching Collection Summary data for Session Id : " + recentSessionKey);
			itemDataMapAsList = getCollectionSummaryData(traceId, assessmentId, recentSessionKey, itemDataMapAsList, isSecure);
		} else {
			logger.info("Recent session is unavailable for collection : " + assessmentId +" and user : "+userUid);
		}
		responseParamDTO.setContent(itemDataMapAsList);
		return responseParamDTO;
	}
	
	private List<Map<String, Object>> getCollectionSummaryData(String traceId, String collectionGooruId, String sessionId, List<Map<String, Object>> itemDataMapAsList, boolean isSecure) {

		//Fetch collection items
		List<Map<String, Object>> itemColumnResult = getContentItems(traceId, collectionGooruId, null, false,null,null);
		StringBuffer resourceGooruOids = getBaseService().getCommaSeparatedIds(itemColumnResult, ApiConstants.GOORUOID);

		//Resource metadata
		List<Map<String, Object>> rawDataMapAsList = getResourceData(traceId, isSecure, resourceGooruOids.toString(), DataUtils.getCollectionSummaryResourceColumns(), ApiConstants.RESOURCE);
		//Usage Data
		Set<String> columnSuffix = DataUtils.getSessionActivityMetricsMap().keySet();
		Collection<String> columns = getBaseService().generateCommaSeparatedStringToKeys(ApiConstants.TILDA, resourceGooruOids.toString(), columnSuffix);
		List<Map<String,Object>> usageDataList = getSessionActivityMetrics(traceId, getBaseService().convertStringToCollection(sessionId), ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), columns, resourceGooruOids.toString());
//		List<Map<String,Object>> usageDataList = getCollectionActivityMetrics(traceId, getBaseService().convertStringToCollection(sessionId), ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), columns, null, false, itemGooruOids.toString(), false);
		//Question meta 
		List<Map<String,Object>> answerRawData = getQuestionMetaData(traceId,collectionGooruId);
		rawDataMapAsList = getBaseService().leftJoin(itemColumnResult, rawDataMapAsList, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
		itemDataMapAsList = getBaseService().leftJoin(rawDataMapAsList, usageDataList, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
		itemDataMapAsList = getBaseService().leftJoin(itemDataMapAsList, answerRawData, ApiConstants.GOORUOID, ApiConstants.GOORUOID);
		
		//Fetch Teacher Detail
		StringBuffer teacherUId = getBaseService().getCommaSeparatedIds(itemDataMapAsList, ApiConstants.FEEDBACKPROVIDER);
		if (teacherUId.length() > 0) {
			String teacherUid = ApiConstants.STRING_EMPTY;
			String[] teacherid = teacherUId.toString().split(COMMA);
			for (String ids : teacherid) {
				if (StringUtils.isNotBlank(ids)) {
					teacherUid = ids;
					break;
				}
			}
			List<Map<String,Object>> teacherData = new ArrayList<Map<String,Object>>();
			Map<String,Object> userMetaInfo = getUserMetaInfo(traceId,teacherUid);
			teacherData.add(userMetaInfo);
			itemDataMapAsList = getBaseService().leftJoin(itemDataMapAsList, teacherData, ApiConstants.FEEDBACKPROVIDER, ApiConstants.FEEDBACKPROVIDER);
		}
		return itemDataMapAsList;
	}
	
	private Map<String,Object> getUserMetaInfo(String traceId, String userId){
		Map<String,Object> userMetaInfo = new HashMap<String,Object>();
		Collection<String> userColumns = new ArrayList<String>();
		userColumns.add(ApiConstants.USERNAME);
		userColumns.add(ApiConstants.GOORUUID);
		OperationResult<ColumnList<String>> userDetails = getCassandraService().read(traceId, ColumnFamily.USER.getColumnFamily(), userId,userColumns);
		if(!userDetails.getResult().isEmpty()){
			ColumnList<String> userColumnList = userDetails.getResult();
			userMetaInfo.put(ApiConstants.USER_UID, userColumnList.getStringValue(ApiConstants.GOORUUID, null));
			userMetaInfo.put(ApiConstants.USER_NAME, userColumnList.getStringValue(ApiConstants.USERNAME, null));
		}
		return userMetaInfo;
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
								if (DataUtils.getStringColumns().containsKey(metricName) && !metricName.matches(ApiConstants.OPTIONS)) {
									usageMap.put(DataUtils.getStringColumns().get(metricName), metricRow.getColumns().getStringValue(column.trim(), null));
								} else if (metricName.matches(ApiConstants.OPTIONS) && StringUtils.isNotBlank(metricRow.getColumns().getStringValue(column.trim(), null)) && !metricRow.getColumns().getStringValue(column.trim(), null).equalsIgnoreCase(ApiConstants.SKIPPED)) {
									optionsAsMap.put(metricRow.getColumns().getStringValue(column.trim(), null), 0);
								} else if (DataUtils.getLongColumns().containsKey(metricName)) {
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
