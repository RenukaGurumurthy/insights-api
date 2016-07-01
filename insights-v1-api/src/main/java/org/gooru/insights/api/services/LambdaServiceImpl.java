package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.SessionTaxonomyActivity;
import org.gooru.insights.api.models.StudentsClassActivity;
import org.gooru.insights.api.utils.InsightsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@SuppressWarnings({ "unused", "unused" })
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

		Map<String, Map<String, List<StudentsClassActivity>>> groupResultList = resultList.stream()
				.collect(Collectors.groupingBy(StudentsClassActivity::getUserUid, Collectors.groupingBy(o -> StudentsClassActivity.aggregateDepth(o, aggregateLevel))));


		return groupResultList.entrySet().stream()
				.map(fo -> aggregateStudentClassActivity(fo, collectionType, aggregateLevel)).collect(Collectors.toList());
	}

	
	/**
	 * This is method will aggregate data based on different collection types. Current supported collection type are collection and assessment
	 * @param resultList
	 * @param collectionType
	 * @param aggregateLevel
	 * @return
	 */
	 @Override
  public List<Map<String, List<Map<String, List<StudentsClassActivity>>>>> aggregateStudentsClassActivityBothData(List<StudentsClassActivity> resultList,
          String collectionType, String aggregateLevel) {
    Map<String, Map<String, Map<String, List<StudentsClassActivity>>>> groupResultList = resultList.stream()
            .collect(Collectors.groupingBy(StudentsClassActivity::getUserUid, Collectors.groupingBy(StudentsClassActivity::getCollectionType,
                    Collectors.groupingBy(o -> StudentsClassActivity.aggregateDepth(o, aggregateLevel)))));

    List<Map<String, List<Map<String, List<StudentsClassActivity>>>>> finalData = groupResultList.entrySet().stream()
            .map(fo -> aggregateStudentClassActivityByUser(fo, collectionType, aggregateLevel)).collect(Collectors.toList());

    return finalData;
  }
	 
	@Override
	public List<StudentsClassActivity> applyFiltersInStudentsClassActivity(List<StudentsClassActivity> resultList,
          String collectionType, String userUid) {
    List<StudentsClassActivity> filteredList = null;
    
    if(ApiConstants.BOTH.equalsIgnoreCase(collectionType)){
      if(StringUtils.isNotBlank(userUid)){
        filteredList = resultList.stream().filter(object -> object.getUserUid().equalsIgnoreCase(userUid) && ((object.getCollectionType().equalsIgnoreCase(ApiConstants.COLLECTION))
                || (object.getCollectionType().equalsIgnoreCase(ApiConstants.ASSESSMENT) && object.getAttemptStatus().equals(ApiConstants.COMPLETED))))
                .collect(Collectors.toList());
      }else{
        filteredList = resultList.stream().filter(object -> (object.getCollectionType().equalsIgnoreCase(ApiConstants.COLLECTION))
                || (object.getCollectionType().equalsIgnoreCase(ApiConstants.ASSESSMENT) && object.getAttemptStatus().equals(ApiConstants.COMPLETED)))
                .collect(Collectors.toList());
      }
    }else if(ApiConstants.COLLECTION.equalsIgnoreCase(collectionType)){
      if(StringUtils.isNotBlank(userUid)){
        filteredList =
                resultList.stream().filter(object -> object.getCollectionType().matches(collectionType) && object.getUserUid().equalsIgnoreCase(userUid)).collect(Collectors.toList());
      }else{
        filteredList =
                resultList.stream().filter(object -> object.getCollectionType().matches(collectionType)).collect(Collectors.toList());
      }
    }else if(ApiConstants.ASSESSMENT.equalsIgnoreCase(collectionType)){
      if(StringUtils.isNotBlank(userUid)){
        filteredList =
                resultList.stream().filter(object -> object.getCollectionType().matches(collectionType) && object.getUserUid().equalsIgnoreCase(userUid)
                        && object.getAttemptStatus().equals(ApiConstants.COMPLETED)).collect(Collectors.toList());
      }else{
        filteredList =
                resultList.stream().filter(object -> object.getCollectionType().matches(collectionType) && object.getAttemptStatus().equals(ApiConstants.COMPLETED)).collect(Collectors.toList());
      }
    }
    return filteredList;
  }

	private void aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList,  Integer depth1, Integer depth2) {
	    long startTime =  System.currentTimeMillis();

	    Map<Object, Map<Object, List<ContentTaxonomyActivity>>> data = resultList.stream().collect(Collectors.groupingBy(object -> ContentTaxonomyActivity.taxonomyDepthField(object,depth1), Collectors.groupingBy(object -> ContentTaxonomyActivity.taxonomyDepthField(

			object,depth2))));
	    List<List<ContentTaxonomyActivity>> resultData = data.entrySet().stream().map(firstLevelObject -> firstLevelObject.getValue().entrySet().stream().map(secondLevelObject -> secondLevelObject.getValue().stream().reduce((f1,f2) -> getContentTaxonomyActivity(f1,f2, depth2)).get()).collect(Collectors.toList())).collect(Collectors.toList());
	    resultList.clear();
		for (List<ContentTaxonomyActivity> aResultData : resultData) {
			resultList.addAll(aResultData);
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
		object1.setCollectionType(object1.getCollectionType());
		object1.setScore(sum(object1.getScore(), object2.getScore()));
		object1.setReaction(sum(object1.getReaction(), object2.getReaction()));
		if (ApiConstants.COLLECTION.equals(object1.getCollectionType()) && ApiConstants.COLLECTION.equals(object2.getCollectionType())) {
			object1.setCollectionId(object1.getCollectionId());
			object1.setViews(sum(object1.getViews(), object2.getViews()));
		}else if (ApiConstants.ASSESSMENT.equals(object1.getCollectionType()) && ApiConstants.ASSESSMENT.equals(object2.getCollectionType())){
			object1.setAssessmentId(object1.getAssessmentId());
			object1.setAttempts(sum(object1.getAttempts(), object2.getAttempts()));
		}else if (ApiConstants.ASSESSMENT.equals(object1.getCollectionType()) && ApiConstants.COLLECTION.equals(object2.getCollectionType())){
		  object1.setAssessmentId(object1.getAssessmentId());
      object1.setAttempts(sum(object1.getAttempts(), object2.getViews()));
		}else if (ApiConstants.COLLECTION.equals(object1.getCollectionType()) && ApiConstants.ASSESSMENT.equals(object2.getCollectionType())){
		  object1.setCollectionId(object1.getCollectionId());
      object1.setAttempts(sum(object1.getViews(), object2.getAttempts()));
		}
		object1.setTimeSpent(sum(object1.getTimeSpent(), object2.getTimeSpent()));
		object1.setCompletedCount(1L);
		return object1;
	}

	private StudentsClassActivity customizeFieldsInStudentClassActivity(StudentsClassActivity object1, String collectionType, String level) {
	
	  object1.setTotalCount(classService.getCulCollectionCount(object1.getClassId(), StudentsClassActivity.aggregateDepth(object1, level), object1.getCollectionType()));
		switch(level) {
			case ApiConstants.UNIT:
				object1.setLessonId(null);
				object1.setCollectionId(null);
				object1.setAssessmentId(null);
				break;
			case ApiConstants.LESSON:
				object1.setUnitId(null);
				object1.setCollectionId(null);
				object1.setAssessmentId(null);
				break;
			case ApiConstants.CONTENT:
			  //TODO: Need to fix total count should come from Cassandra
			  object1.setTotalCount(1L);
				object1.setLessonId(null);
				object1.setUnitId(null);
				if(ApiConstants.ASSESSMENT.equalsIgnoreCase(object1.getCollectionType())) {
					object1.setCollectionId(null);
				}else if(ApiConstants.COLLECTION.equalsIgnoreCase(object1.getCollectionType())){
					object1.setAssessmentId(null);
				}
				break;

		}
		object1.setScoreInPercentage(object1.getScore(), object1.getCompletedCount());
		object1.setReaction(Math.round((double)((object1 != null ? object1.getReaction() : 0) / object1.getCompletedCount())));
		object1.setScore(null);
		object1.setClassId(null);
		object1.setCollectionType(null);
		object1.setUserUid(null);
		return object1;
	}

	private Map<String,List<StudentsClassActivity>> aggregateStudentClassActivity(Map.Entry<String, Map<String, List<StudentsClassActivity>>> fo, String collectionType, String aggregateLevel) {
		Map<String, List<StudentsClassActivity>> studentActivity = new HashMap<>();
		List<StudentsClassActivity> studentClassActivity = fo.getValue().entrySet().stream()
				.map(sob -> sob.getValue().stream().map(defaultValueForAggregation -> {defaultValueForAggregation.setCompletedCount(1L);  return defaultValueForAggregation;})
						.reduce((o1, o2) -> getStudentsClassActivity(o1, o2,aggregateLevel))
						.map(additionalCalculation -> customizeFieldsInStudentClassActivity(additionalCalculation, collectionType, aggregateLevel)).get())
				.collect(Collectors.toList());
		studentActivity.put((String)fo.getKey(), studentClassActivity);
		return studentActivity;
	}

  private Map<String, List<Map<String, List<StudentsClassActivity>>>> aggregateStudentClassActivityByUser(
          Entry<String, Map<String, Map<String, List<StudentsClassActivity>>>> fo, String collectionType, String aggregateLevel) {

    Map<String, List<Map<String, List<StudentsClassActivity>>>> studentActivity = new HashMap<>();
    studentActivity.put((String) fo.getKey(), fo.getValue().entrySet().stream()
            .map(o -> aggregateStudentClassActivity(o, collectionType, aggregateLevel)).collect(Collectors.toList()));
    return studentActivity;

  }
	
	@Override
	public List<SessionTaxonomyActivity> aggregateSessionTaxonomyActivity(List<SessionTaxonomyActivity> sessionTaxonomyActivity, String levelType) {

		List<SessionTaxonomyActivity> resultSet = new ArrayList<>();

		Map<Object, Map<Object, List<SessionTaxonomyActivity>>> groupedData = sessionTaxonomyActivity.parallelStream().collect(Collectors.groupingBy(


			SessionTaxonomyActivity::getStandardsId,Collectors.groupingBy(SessionTaxonomyActivity::getLearningTargetsId)));

		List<List<SessionTaxonomyActivity>>  standardsObj = groupedData.entrySet().parallelStream().map(data -> aggregateLearningTargetsObj(data.getValue())).collect(Collectors.toList());

		standardsObj.forEach(resultSet::addAll);

		return resultSet;
	}

	private List<SessionTaxonomyActivity> aggregateLearningTargetsObj(Map<Object, List<SessionTaxonomyActivity>> obj){
			return obj.entrySet().parallelStream().map(ob -> generateStandardsMetrics(ob.getValue())).collect(Collectors.toList());
	}

	private SessionTaxonomyActivity generateStandardsMetrics(List<SessionTaxonomyActivity> obj){
		SessionTaxonomyActivity standardsObj = obj.parallelStream().map(this::generateQuestionObj).reduce(
			this::getSessionTaxonomyActivity).map(ob -> removeUnwantedFields(ob, ApiConstants.DOMAIN)).get();
		standardsObj.setQuestions(obj);
		return standardsObj;
	}
	private SessionTaxonomyActivity generateQuestionObj(SessionTaxonomyActivity sessionTaxonomyActivity){
		try {
			return (SessionTaxonomyActivity) sessionTaxonomyActivity.clone();
		} catch (CloneNotSupportedException e) {
			LOG.error("Error while cloning SessionTaxonomyActivity {}", e);
		}
		return null;
	}
	@Override
	public List<SessionTaxonomyActivity> aggregateSessionTaxonomyActivityByGooruOid(List<SessionTaxonomyActivity> sessionTaxonomyActivity) {

		Map<String, List<SessionTaxonomyActivity>> groupedData = sessionTaxonomyActivity.parallelStream().collect(Collectors.groupingBy(


			SessionTaxonomyActivity::getResourceId));

		sessionTaxonomyActivity = groupedData.entrySet().parallelStream().map(taxonomy -> taxonomy.getValue().parallelStream().reduce(


			this::getSessionTaxonomyActivityByTax).get()).map(f1 -> {removeUnwantedFields(f1);return f1;}).collect(Collectors.toList());

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
		obj1.setScore(obj1.getScore()+ obj2.getScore());
		obj1.setAttempts(obj1.getAttempts() + obj2.getAttempts());
		obj1.setTimespent(obj1.getTimespent() + obj2.getTimespent());
		obj1.setReaction(obj1.getReaction() + obj2.getReaction());
		obj1.setTotalAttemptedQuestions(obj1.getTotalAttemptedQuestions() + obj2.getTotalAttemptedQuestions());
		return obj1;
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
		
		obj1.setScore(Math.round((double)(obj1.getScore()/obj1.getTotalAttemptedQuestions())));
		obj1.setTotalAttemptedQuestions(null);
		obj1.setSubjectId(null);
		obj1.setCourseId(null);
		obj1.setDomainId(null);
		if(obj1.getLearningTargetsId() != null && !obj1.getLearningTargetsId().equalsIgnoreCase(ApiConstants.NA)){
			obj1.setStandardsId(null);
		}else{
			obj1.setLearningTargetsId(null);
		}
		obj1.setQuestionId(null);
		obj1.setQuestionType(null);
		obj1.setAnswerStatus(null);
		obj1.setAttempts(null);
		obj1.setReaction(null);
		return obj1;
	}
}
