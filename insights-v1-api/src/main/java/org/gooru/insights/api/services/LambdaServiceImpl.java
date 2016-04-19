package org.gooru.insights.api.services;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.SessionTaxonomyActivity;
import org.gooru.insights.api.models.StudentsClassActivity;
import org.gooru.insights.api.utils.InsightsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class LambdaServiceImpl implements LambdaService{

	private static final Logger LOG = LoggerFactory.getLogger(LambdaServiceImpl.class);
	
	@Autowired
	private ClassService classService;

	@Override
	public List<ContentTaxonomyActivity> aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList,  Integer depth) {
		
		long startTime =  System.currentTimeMillis();
		Map<String, List<ContentTaxonomyActivity>> data = resultList.parallelStream().collect(Collectors.groupingBy(object -> ContentTaxonomyActivity.taxonomyDepthField(object,depth)));
		resultList = data.entrySet().parallelStream().map(taxonomy -> taxonomy.getValue().parallelStream().reduce((f1,f2) -> getContentTaxonomyActivity(f1,f2, depth)).get()).collect(Collectors.toList());
		long endTime = System.currentTimeMillis();
		InsightsLogger.debug("content taxonomy activity 1st level aggregation Time:"+(endTime - startTime)+" record count:"+resultList.size());
		return resultList;
	}
	
	@Override
	public List<Map<String, List<StudentsClassActivity>>> aggregateStudentsClassActivityData(List<StudentsClassActivity> resultList, String collectionType, String aggregateLevel) {
		
		Map<Object, Map<Object, List<StudentsClassActivity>>> groupResultList = resultList.stream()
				.collect(Collectors.groupingBy(o -> o.getUserUid(), Collectors.groupingBy(o -> StudentsClassActivity.aggregateDepth(o, aggregateLevel))));

		List<Map<String, List<StudentsClassActivity>>> aggregatedResultList = groupResultList.entrySet().stream()
				.map(fo -> aggregateStudentClassActivity(fo, collectionType, aggregateLevel)).collect(Collectors.toList());
		return aggregatedResultList;
	}
	
	@Override
	public List<StudentsClassActivity> applyFiltersInStudentsClassActivity(List<StudentsClassActivity> resultList,  String collectionType) {
		String collectionTypeDuplicate = collectionType.equalsIgnoreCase("both") ? "collection|assessment" : collectionType;
		List<StudentsClassActivity> filteredList = resultList.stream()
				.filter(object -> object.getCollectionType().matches(collectionType))
				.collect(Collectors.toList());
		return filteredList;
	}
	
	private void aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList,  Integer depth1, Integer depth2) {
	    long startTime =  System.currentTimeMillis();
	                
	    Map<Object, Map<Object, List<ContentTaxonomyActivity>>> data = resultList.stream().collect(Collectors.groupingBy(object -> ContentTaxonomyActivity.taxonomyDepthField(object,depth1), Collectors.groupingBy(object -> ContentTaxonomyActivity.taxonomyDepthField((ContentTaxonomyActivity) object,depth2))));
	    List<List<ContentTaxonomyActivity>> resultData = data.entrySet().stream().map(firstLevelObject -> firstLevelObject.getValue().entrySet().stream().map(secondLevelObject -> secondLevelObject.getValue().stream().reduce((f1,f2) -> getContentTaxonomyActivity(f1,f2, depth2)).get()).collect(Collectors.toList())).collect(Collectors.toList());
	    resultList.clear();
	    Iterator<List<ContentTaxonomyActivity>> dataList = resultData.iterator();
	    while(dataList.hasNext()) {
	         resultList.addAll(dataList.next());
	    }
	    long endTime = System.currentTimeMillis();
	    InsightsLogger.debug("content taxonomy activity 2nd level aggregation Time:"+(endTime - startTime));
	}
	
	private ContentTaxonomyActivity getContentTaxonomyActivity(ContentTaxonomyActivity  object1, ContentTaxonomyActivity  object2, Integer depth) {
		ContentTaxonomyActivity  contentTaxonomyActivity = new ContentTaxonomyActivity(object1,depth);
			contentTaxonomyActivity.setScore(sum(object1.getScore(), object2.getScore()));
			contentTaxonomyActivity.setAttempts(sum(object1.getAttempts(), object2.getAttempts()));
			contentTaxonomyActivity.setViews(sum(object1.getViews(), object2.getViews()));
			contentTaxonomyActivity.setTimespent(sum(object1.getTimespent(), object2.getTimespent()));
		return contentTaxonomyActivity;
	}
	
	private StudentsClassActivity getStudentsClassActivity(StudentsClassActivity object1, StudentsClassActivity object2,
			String level) {
		StudentsClassActivity studentsClassActivity = object1;
		studentsClassActivity.setCollectionId(object1.getCollectionId());
		studentsClassActivity.setCollectionType(object1.getCollectionType());
		studentsClassActivity.setScore(sum(object1.getScore(), object2.getScore()));
		studentsClassActivity.setReaction(sum(object1.getReaction(), object2.getReaction()));
		studentsClassActivity.setViews(sum(object1.getViews(), object2.getViews()));
		studentsClassActivity.setTimeSpent(sum(object1.getTimeSpent(), object2.getTimeSpent()));
		studentsClassActivity.setCompletedCount(1L);
		return studentsClassActivity;
	}
	
	private StudentsClassActivity customizeFieldsInStudentClassActivity(StudentsClassActivity object1, String collectionType, String level) {
		switch(level) {
			case ApiConstants.UNIT:
				object1.setLessonId(null);
				object1.setCollectionId(null);
				break;
			case ApiConstants.LESSON:
				object1.setUnitId(null);
				object1.setCollectionId(null);
				break;
			case ApiConstants.CONTENT:
				object1.setLessonId(null);
				object1.setUnitId(null);
				if(ApiConstants.ASSESSMENT.equalsIgnoreCase(collectionType)) {
					object1.setAssessmentId(object1.getCollectionId());
					object1.setCollectionId(null);
				}
				break;
				
		}
		object1.setTotalCount(classService.getCulCollectionCount(object1.getClassId(), StudentsClassActivity.aggregateDepth(object1, level), object1.getCollectionType()));
		object1.setScoreInPercentage(object1.getScore(), object1.getCompletedCount());
		object1.setReaction((object1 != null ? object1.getReaction() : 0) / object1.getCompletedCount());
		object1.setScore(null);
		object1.setClassId(null);
		object1.setCollectionType(null);
		object1.setUserUid(null);
		return object1;
	}
	
	private Map<String,List<StudentsClassActivity>> aggregateStudentClassActivity(Map.Entry<Object, Map<Object, List<StudentsClassActivity>>> fo, String collectionType, String aggregateLevel) {
		Map<String, List<StudentsClassActivity>> studentActivity = new HashMap<>();
		List<StudentsClassActivity> studentClassActivity = fo.getValue().entrySet().stream()
				.map(sob -> sob.getValue().stream().map(defaultValueForAggregation -> {defaultValueForAggregation.setCompletedCount(1L);  return defaultValueForAggregation;})
						.reduce((o1, o2) -> getStudentsClassActivity(o1, o2,aggregateLevel))
						.map(additionalCalculation -> customizeFieldsInStudentClassActivity(additionalCalculation, collectionType, aggregateLevel)).get())
				.collect(Collectors.toList());
		studentActivity.put((String)fo.getKey(), studentClassActivity);
		return studentActivity;
	}
	
	@Override
	public List<SessionTaxonomyActivity> aggregateSessionTaxonomyActivity(List<SessionTaxonomyActivity> sessionTaxonomyActivity, String levelType) {
		
		Map<String, List<SessionTaxonomyActivity>> groupedData = sessionTaxonomyActivity.parallelStream().collect(Collectors.groupingBy(object -> SessionTaxonomyActivity.getGroupByField(object, levelType)));
		List<SessionTaxonomyActivity> sessionTaxonomyActivityMetrics = groupedData.entrySet().parallelStream().map( data -> data.getValue().stream().reduce((obj1,obj2) -> getSessionTaxonomyActivity(obj1, obj2)).map(obj1 -> { removeUnwantedFields(obj1, levelType);obj1.setQuestions(data.getValue()); return obj1; }).get()).collect(Collectors.toList());
		return sessionTaxonomyActivityMetrics;
	}
	
	@Override
	public List<SessionTaxonomyActivity> aggregateSessionTaxonomyActivityByGooruOid(List<SessionTaxonomyActivity> sessionTaxonomyActivity) {

		Map<String, List<SessionTaxonomyActivity>> groupedData = sessionTaxonomyActivity.parallelStream().collect(Collectors.groupingBy(object -> object.getResourceId()));		
		
		sessionTaxonomyActivity = groupedData.entrySet().parallelStream().map(taxonomy -> taxonomy.getValue().parallelStream().reduce((f1,f2) -> getSessionTaxonomyActivityByTax(f1,f2)).get()).map(f1 -> {removeUnwantedFields(f1);return f1;}).collect(Collectors.toList());
		
		return sessionTaxonomyActivity;
	}
	
	private Long sum(Long obj1, Long obj2) {
		if(obj1 != null) {
			if(obj2 != null)
				return obj1+obj2;
			else 
				return obj1;
		} else if (obj2 != null) 
			return obj2;
		
		return null;
	}
	
	private SessionTaxonomyActivity getSessionTaxonomyActivity(SessionTaxonomyActivity obj1, SessionTaxonomyActivity obj2) {
		
		SessionTaxonomyActivity data = new SessionTaxonomyActivity();
		data.setScore(obj1.getScore()+ obj2.getScore());
		data.setAttempts(obj1.getAttempts() + obj2.getAttempts());
		data.setTimespent(obj1.getTimespent() + obj2.getTimespent());
		data.setReaction(obj1.getReaction() + obj2.getReaction());
		data.setTotalAttemptedQuestions(obj1.getTotalAttemptedQuestions() + obj2.getTotalAttemptedQuestions());
		data.setSubjectId(obj1.getSubjectId());
		data.setCourseId(obj1.getCourseId());
		data.setDomainId(obj1.getDomainId());
		data.setStandardsId(obj1.getStandardsId());
		data.setLearningTargetsId(obj1.getLearningTargetsId());
		return data;
	}
	
	private SessionTaxonomyActivity getSessionTaxonomyActivityByTax(SessionTaxonomyActivity obj1,
			SessionTaxonomyActivity obj2) {
		obj1.getSubjectIds().addAll(obj2.getSubjectIds());
		obj1.getCourseIds().addAll(obj2.getCourseIds());
		obj1.getDomainIds().addAll(obj2.getDomainIds());
		obj1.getStandardsIds().addAll(obj2.getStandardsIds());
		obj1.getLearningTargetsIds().addAll(obj2.getLearningTargetsIds());
		
		return obj1;
	}
	private SessionTaxonomyActivity removeUnwantedFields(SessionTaxonomyActivity obj1) {
		obj1.setSubjectId(null);
		obj1.setCourseId(null);
		obj1.setDomainId(null);
		obj1.setStandardsId(null);
		obj1.setLearningTargetsId(null);
		obj1.setTotalAttemptedQuestions(null);
		return obj1;
	}
	private SessionTaxonomyActivity removeUnwantedFields(SessionTaxonomyActivity obj1, String depthLevel) {
		
		obj1.setScore(obj1.getScore()/obj1.getTotalAttemptedQuestions());
		obj1.setTotalAttemptedQuestions(null);
		switch(depthLevel) {
		case ApiConstants.SUBJECT:
			obj1.setSubjectId(null);
			obj1.setDomainId(null);
			obj1.setStandardsId(null);
			obj1.setLearningTargetsId(null);
			break;
		case ApiConstants.COURSE:
			obj1.setSubjectId(null);
			obj1.setCourseId(null);
			obj1.setStandardsId(null);
			obj1.setLearningTargetsId(null);
			break;
		case ApiConstants.DOMAIN:
			obj1.setSubjectId(null);
			obj1.setCourseId(null);
			obj1.setDomainId(null);
			obj1.setLearningTargetsId(null);
			break;
		case ApiConstants.STANDARDS:
			obj1.setSubjectId(null);
			obj1.setCourseId(null);
			obj1.setDomainId(null);
			obj1.setStandardsId(null);
			break;
		}
		obj1.setQuestionId(null);
		obj1.setQuestionType(null);
		obj1.setAnswerStatus(null);
		return obj1;
	}
}
