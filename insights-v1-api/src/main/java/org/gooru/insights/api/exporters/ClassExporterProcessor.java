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
	
	public File exportClassUserUsageReport(String traceId, String classId, String courseId, String unitId, String lessonId, String collectionType, String collectionId) throws ParseException, IOException{
		
		List<Map<String,Object>> resources = getClassService().getContentItems(traceId,collectionId,null,true);
		List<Map<String,Object>> students = getClassService().getStudents(traceId, classId);
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
				if(resource.get(ApiConstants.RESOURCE_TYPE) != null){
					if(resource.get(ApiConstants.RESOURCE_TYPE).toString().matches(ApiConstants.ASSESSMENT_MATCH)){
						asessmentData = true;
					}
				}
				Collection<String> rowKeys = new ArrayList<String>();
				rowKeys.add(getBaseService().appendTilda(SessionAttributes.RS.getSession(),classId,courseId,unitId,lessonId,collectionId, student.get(ApiConstants.USERUID).toString()));
				List<String> sessionIds = getClassService().getSessions(traceId,rowKeys);
				String sessionId = ApiConstants.DATA;
				if(sessionIds.size() > 0){
					sessionId = sessionIds.get(0);
				}
				ColumnList<String> resourceActivity = getCassandraService().read(traceId, ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionId).getResult();
				if(asessmentData){
					keyPrefix = getBaseService().buildString(ApiConstants.QUESTION,asessmentCount,ApiConstants.HYPHEN);
					if(resourceActivity == null || resourceActivity.isEmpty() ) {
						usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ANSWER),ApiConstants.HYPHEN);
						usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.CORRECT_INCORRECT),ApiConstants.HYPHEN);
						usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ATTEMPTS),ApiConstants.HYPHEN);
		
					}else{
						String answer = resourceActivity.getStringValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ExportFileConstants.OPTIONS), ApiConstants.HYPHEN);
						String oeText = resourceActivity.getStringValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ExportFileConstants.CHOICE), ApiConstants.HYPHEN);
						String scoreStatus = ApiConstants.HYPHEN;
						if(oeText.equalsIgnoreCase(ApiConstants.HYPHEN)){
							int storedScore = resourceActivity.getIntegerValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants.SCORE), -1);
							if(storedScore > 0 && answer.equalsIgnoreCase(ExportFileConstants.SKIPPED)){
								scoreStatus = ExportFileConstants.CORRECT;
							}else if(storedScore == 0){
								scoreStatus = ExportFileConstants.IN_CORRECT;
							}
						}else{
							try {
								answer = URLDecoder.decode(oeText, ApiConstants.UTF8);
							} catch (Exception e) {
								InsightsLogger.error(traceId, e);
							}
						}
						Long storedAttempts = resourceActivity.getLongValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants.ATTEMPTS), 0L);
						String attempts = (storedAttempts > 0)? ApiConstants.STRING_EMPTY+storedAttempts: ApiConstants.HYPHEN;
						usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ANSWER),answer);
						usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.CORRECT_INCORRECT),scoreStatus);
						usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.ATTEMPTS),attempts);
					}
					asessmentCount++;
				}else{
					keyPrefix = getBaseService().buildString(ApiConstants.RESOURCE,resourceCount,ApiConstants.HYPHEN);
					resourceCount++;
				}
				if(resourceActivity == null  || resourceActivity.isEmpty() ){
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.TIME_SPENT),ApiConstants.HYPHEN);
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.VIEWS),ApiConstants.HYPHEN);
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.REACTION),ApiConstants.HYPHEN);
				}else{
					Long timeSpent = resourceActivity.getLongValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants._TIME_SPENT), 0L);
					String FormatedTimeSpent = getBaseService().getHourlyBasedTimespent(timeSpent);
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.TIME_SPENT),FormatedTimeSpent);
	
					Long storedViews = resourceActivity.getLongValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants.VIEWS), 0L);
					String views =  (storedViews > 0) ? ApiConstants.STRING_EMPTY+storedViews : ApiConstants.HYPHEN;
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.VIEWS),views);
	
					long storedReaction = resourceActivity.getLongValue(getBaseService().buildString(resource.get(ApiConstants.GOORUOID),ApiConstants.TILDA,ApiConstants.RA), -1L);
					String reaction = null;
					if(storedReaction <0){
						reaction = ApiConstants.HYPHEN;
					}else{
					reaction =  reactionMapper.get(storedReaction);
					}
					usageData.put(getBaseService().buildString(keyPrefix,ExportFileConstants.REACTION),reaction);
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
		String fileAppPathName = getCsvFileGenerator().generateCSVReport(false,fileName, resourceUsage);
		return new File(fileAppPathName);
				
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
