package org.gooru.insights.api.exporters;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.SessionAttributes;
import org.gooru.insights.api.constants.ExportFileConstants;
import org.gooru.insights.api.models.InsightsConstant.ColumnFamily;
import org.gooru.insights.api.services.BaseService;
import org.gooru.insights.api.services.CassandraService;
import org.gooru.insights.api.services.ClassService;
import org.gooru.insights.api.utils.DataUtils;
import org.gooru.insights.api.utils.InsightsLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.astyanax.model.ColumnList;

@Component
public class ClassExporterProcessor {

	@Autowired
	ClassService classService;
	
	@Autowired
	BaseService baseService;
	
	@Autowired
	CassandraService cassandraService;
	
	@Autowired
	CSVFileGenerator csvFileGenerator;
	
	private Map<Integer,String> reactionMapper;

	@PostConstruct
	private void init(){
		reactionMapper = new HashMap<Integer,String>();	
		reactionMapper.put(1, ApiConstants.reactions.ONE.reaction());
		reactionMapper.put(2, ApiConstants.reactions.TWO.reaction());
		reactionMapper.put(3, ApiConstants.reactions.THREE.reaction());
		reactionMapper.put(4, ApiConstants.reactions.FOUR.reaction());
		reactionMapper.put(5, ApiConstants.reactions.FIVE.reaction());
		reactionMapper.put(10, ApiConstants.reactions.TEN.reaction());
	}
	
	public File exportClassUserUsageReport(String classId, String courseId, String unitId, String lessonId, String collectionType, String collectionId) throws ParseException, IOException{
		
		List<Map<String,Object>> resources = getClassService().getContentItems(collectionId,null,true,null,DataUtils.getResourceFields());
		List<Map<String,Object>> students = getClassService().getStudents(classId);
		resources = getBaseService().sortBy(resources, ApiConstants.SEQUENCE, ApiConstants.ASC);
		students = getBaseService().sortBy(students, ApiConstants.USER_NAME, ApiConstants.ASC);
		List<Map<String,Object>> questionUsage = new ArrayList<Map<String,Object>>();
		List<Map<String,Object>> resourceUsage = new ArrayList<Map<String,Object>>();
		String fileName = ExportFileConstants.PROGRESS_FILE_NAME+System.currentTimeMillis()+ApiConstants.CSV_EXT;
		
		/**
		 * Fetch session data at users level
		 */
		for(Map<String,Object> student : students){

			int resourceCount = 1;
			int asessmentCount = 1;
			Map<String,Object> studentQuestionUsage = new LinkedHashMap<String,Object>();
			Map<String,Object> studentCollectionUsage = new LinkedHashMap<String,Object>();
			studentQuestionUsage.put(ExportFileConstants.QUESTION_DATA, ApiConstants.STRING_EMPTY);
			studentQuestionUsage.put(ExportFileConstants.STUDENT, student.get(ApiConstants.USER_NAME));
			studentCollectionUsage.put(ExportFileConstants.RESOURCE_DATA, ApiConstants.STRING_EMPTY);
			studentCollectionUsage.put(ExportFileConstants.STUDENT, student.get(ApiConstants.USER_NAME));
			
			for(Map<String,Object> resource : resources){
				String keyPrefix = null;
				boolean asessmentData = false;
				Map<String,Object> usageData = new LinkedHashMap<String,Object>();
				if(resource.get(ApiConstants.TYPE) != null){
					if(!resource.get(ApiConstants.TYPE).toString().matches(ApiConstants.COLLECTION_MATCH)){
						asessmentData = true;
					}
				}
				Collection<String> rowKeys = new ArrayList<String>();
				rowKeys.add(getBaseService().appendTilda(SessionAttributes.RS.getSession(),classId,courseId,unitId,lessonId,collectionId, student.get(ApiConstants.USER_UID).toString()));
				List<String> sessionIds = getClassService().getSessions(rowKeys);
				String sessionId = ApiConstants.DATA;
				if(sessionIds.size() > 0){
					sessionId = sessionIds.get(0);
				}
				ColumnList<String> resourceActivity = getCassandraService().read(ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionId).getResult();
				if(asessmentData){
					keyPrefix = getBaseService().buildString(ApiConstants.QUESTION,asessmentCount,ApiConstants.HYPHEN);
					if(resourceActivity == null || resourceActivity.isEmpty() ) {
						usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ANSWER),ApiConstants.HYPHEN);
						usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.CORRECT_INCORRECT),ApiConstants.HYPHEN);
						usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ATTEMPTS),ApiConstants.HYPHEN);
		
					}else{
						fetchQuestionMetrics(keyPrefix,resourceActivity,resource,usageData);
						}
					asessmentCount++;
				}else{
					keyPrefix = getBaseService().buildString(ExportFileConstants.RESOURCE,resourceCount,ApiConstants.HYPHEN);
					resourceCount++;
				}
				if(resourceActivity == null  || resourceActivity.isEmpty() ){
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.VIEWS),ApiConstants.HYPHEN);
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.TIME_SPENT),ApiConstants.HYPHEN);
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.REACTION),ApiConstants.HYPHEN);
				}else{
					Long storedViews = resourceActivity.getLongValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants.VIEWS), 0L);
					String views =  (storedViews > 0) ? ApiConstants.STRING_EMPTY+storedViews : ApiConstants.HYPHEN;
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.VIEWS),views);
					fetchResourceMetrics(keyPrefix,resourceActivity,resource,usageData);
				}
				if(asessmentData){
					studentQuestionUsage.putAll(usageData);
				}else{
					studentCollectionUsage.putAll(usageData);
				}
			}
				questionUsage.add(studentQuestionUsage);
				resourceUsage.add(studentCollectionUsage);
		}
		getCsvFileGenerator().generateCSVReport(true,fileName, questionUsage);
		getCsvFileGenerator().includeEmptyLine(false,fileName, 2);
		return getCsvFileGenerator().generateCSVReport(false,fileName, resourceUsage);
				
	}

	public File exportClassSummaryReport(String collectionId,String sessionId) throws ParseException, IOException{
		
		boolean includCollectionData = false;
		String fileName = ExportFileConstants.SUMMARY_FILE_NAME+System.currentTimeMillis()+ApiConstants.CSV_EXT;
		List<Map<String,Object>> resources = getClassService().getContentItems(collectionId,null,true,null,DataUtils.getResourceFields());
		resources = getBaseService().sortBy(resources, ApiConstants.SEQUENCE, ApiConstants.ASC);
		Map<String,Object> collectionData = new LinkedHashMap<String,Object>();
		Map<String,Object> questionData = new LinkedHashMap<String,Object>();
		Map<String,Object> resourceData = new LinkedHashMap<String,Object>();
		
		Map<String,Object> collectionMetaInfo = new LinkedHashMap<String,Object>();
		getClassService().getResourceMetaData(collectionMetaInfo, null, collectionId,DataUtils.getResourceFields());
		String collectionKeyPrefix = getBaseService().buildString(collectionMetaInfo.get(ApiConstants.TITLE),ApiConstants.HYPHEN);
		collectionData.put(ExportFileConstants.COLLECTION_DATA, ApiConstants.STRING_EMPTY);
		questionData.put(ExportFileConstants.QUESTION_DATA, ApiConstants.STRING_EMPTY);
		resourceData.put(ExportFileConstants.RESOURCE_DATA, ApiConstants.STRING_EMPTY);
		for(Map<String,Object> resource : resources){

			String keyPrefix = getBaseService().buildString(resource.get(ApiConstants.TITLE).toString(),ApiConstants.HYPHEN);
			Map<String,Object> resourceUsage = new LinkedHashMap<String,Object>();
			boolean asessmentData = false;
			if(resource.get(ApiConstants.TYPE) != null){
				if(resource.get(ApiConstants.TYPE).toString().matches(ApiConstants.ASSESSMENT_QUESTION_TYPES)){
					asessmentData = true;
				}
			}
			ColumnList<String> resourceActivity = getCassandraService().read(ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionId).getResult();
			if(!includCollectionData){
				if(resourceActivity == null || resourceActivity.isEmpty() ) {
					collectionData.put(getBaseService().buildString(collectionKeyPrefix,ExportFileConstants.SCORE_IN_PERCENTAGE),ApiConstants.HYPHEN);
					collectionData.put(getBaseService().buildString(collectionKeyPrefix,ExportFileConstants.SCORE),ApiConstants.HYPHEN);
				}else{
					long storedScoreInPercentage = resourceActivity.getLongValue(getBaseService().appendTilda(collectionId,ApiConstants._SCORE_IN_PERCENTAGE), -1L);
					String scoreInPercentage = getBaseService().buildString(ApiConstants.STRING_EMPTY,storedScoreInPercentage);
					if(storedScoreInPercentage < 0){
						scoreInPercentage = ApiConstants.HYPHEN;
					}
					collectionData.put(getBaseService().buildString(collectionKeyPrefix,ExportFileConstants.SCORE_IN_PERCENTAGE),scoreInPercentage);
					long storedScore = resourceActivity.getLongValue(getBaseService().appendTilda(collectionId,ApiConstants.SCORE), -1L);
					String score = getBaseService().buildString(ApiConstants.STRING_EMPTY,storedScore);
					if(storedScore < 0){
						score = ApiConstants.HYPHEN;
					}
					collectionData.put(getBaseService().buildString(collectionKeyPrefix,ExportFileConstants.SCORE),score);

				}
				includCollectionData = true;
			}
			fetchResourceMetrics(keyPrefix,resourceActivity,resource,resourceUsage);			
			if(asessmentData){
				if(resourceActivity == null || resourceActivity.isEmpty() ) {
					resourceUsage.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ANSWER),ApiConstants.HYPHEN);
					resourceUsage.put(getBaseService().buildString(keyPrefix,ExportFileConstants.TIME_SPENT),ApiConstants.HYPHEN);
					resourceUsage.put(getBaseService().buildString(keyPrefix,ExportFileConstants.REACTION),ApiConstants.HYPHEN);
				}else{
					String answer = resourceActivity.getStringValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ExportFileConstants.OPTIONS), ApiConstants.HYPHEN);
					resourceUsage.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ANSWER),answer);
					String oeText = resourceActivity.getStringValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ExportFileConstants.CHOICE), ApiConstants.HYPHEN);
					if(answer.equalsIgnoreCase(ApiConstants.HYPHEN)){
						try {
							answer = URLDecoder.decode(oeText, ApiConstants.UTF8);
						} catch (Exception e) {
							InsightsLogger.error(e);
						}
					}
					resourceUsage.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ANSWER),answer);
				}
				questionData.putAll(resourceUsage);
			}else{
				resourceData.putAll(resourceUsage);
			}
		}
		getCsvFileGenerator().generateCSVReport(true,fileName, collectionData);
		getCsvFileGenerator().includeEmptyLine(false,fileName, 2);
		getCsvFileGenerator().generateCSVReport(false,fileName, questionData);
		getCsvFileGenerator().includeEmptyLine(false,fileName, 2);
		return getCsvFileGenerator().generateCSVReport(false,fileName, resourceData);
	}
	
	private void fetchQuestionMetrics(String keyPrefix,ColumnList<String> resourceActivity,Map<String,Object> resource,Map<String,Object> usageData){
		String answer = resourceActivity.getStringValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ExportFileConstants.OPTIONS), ApiConstants.HYPHEN);
		String oeText = resourceActivity.getStringValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ExportFileConstants.CHOICE), ApiConstants.HYPHEN);
		String scoreStatus = ApiConstants.HYPHEN;
		if(oeText.equalsIgnoreCase(ApiConstants.HYPHEN)){
			long storedScore = resourceActivity.getLongValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants.SCORE), -1L);
			if(storedScore > 0 && answer.equalsIgnoreCase(ExportFileConstants.SKIPPED)){
				scoreStatus = ExportFileConstants.CORRECT;
			}else if(storedScore == 0){
				scoreStatus = ExportFileConstants.IN_CORRECT;
			}
		}else{
			try {
				answer = URLDecoder.decode(oeText, ApiConstants.UTF8);
			} catch (Exception e) {
				InsightsLogger.error(e);
			}
		}
		Long storedAttempts = resourceActivity.getLongValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants.ATTEMPTS), 0L);
		String attempts = (storedAttempts > 0)? ApiConstants.STRING_EMPTY+storedAttempts: ApiConstants.HYPHEN;
		usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ANSWER),answer);
		usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.CORRECT_INCORRECT),scoreStatus);
		usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ATTEMPTS),attempts);
	}
	
	private void fetchResourceMetrics(String keyPrefix,ColumnList<String> resourceActivity,Map<String,Object> resource,Map<String,Object> usageData){
		if(resourceActivity == null || resourceActivity.isEmpty()){
			usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.TIME_SPENT),ApiConstants.HYPHEN);
			usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.REACTION),ApiConstants.HYPHEN);
		}else{
			Long timeSpent = resourceActivity.getLongValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants._TIME_SPENT), 0L);
			String FormatedTimeSpent = getBaseService().getHourlyBasedTimespent(timeSpent);
			usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.TIME_SPENT),FormatedTimeSpent);
	
			long storedReaction = resourceActivity.getLongValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants.RA), -1L);
			String reaction = null;
			if(storedReaction <0){
				reaction = ApiConstants.HYPHEN;
			}else{
			reaction =  reactionMapper.get(storedReaction);
			}
			usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.REACTION),reaction);
		}
	}
	
	public ClassService getClassService() {
		return classService;
	}

	public void setClassService(ClassService classService) {
		this.classService = classService;
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

	public CSVFileGenerator getCsvFileGenerator() {
		return csvFileGenerator;
	}

	public void setCsvFileGenerator(CSVFileGenerator csvFileGenerator) {
		this.csvFileGenerator = csvFileGenerator;
	}
}
