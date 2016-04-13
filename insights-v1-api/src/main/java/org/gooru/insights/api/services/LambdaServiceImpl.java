package org.gooru.insights.api.services;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.StudentsClassActivity;
import org.gooru.insights.api.utils.InsightsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


@Component
public class LambdaServiceImpl implements LambdaService{

	private static final Logger LOG = LoggerFactory.getLogger(LambdaServiceImpl.class);
	
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
	public List<List<StudentsClassActivity>> aggregateStudentsClassActivityData(List<StudentsClassActivity> resultList,  String aggregateLevel) {
		
		Map<Object, Map<Object, List<StudentsClassActivity>>> groupResultList = resultList.stream()
				.collect(Collectors.groupingBy(o -> o.getUserUid(), Collectors.groupingBy(o -> StudentsClassActivity.aggregateDepth(o, aggregateLevel))));

		List<List<StudentsClassActivity>> aggregatedResultList = groupResultList.entrySet().stream()
				.map(fo -> fo.getValue().entrySet().stream()
						.map(sob -> sob.getValue().stream().reduce((o1, o2) -> getStudentsClassActivity(o1, o2,aggregateLevel)).get())
						.collect(Collectors.toList()))
				.collect(Collectors.toList());

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
		StudentsClassActivity studentsClassActivity = new StudentsClassActivity();
		switch (level) {
		case ApiConstants.UNIT:
			studentsClassActivity.setUnitUid(object1.getUnitUid());
			studentsClassActivity.setLessonUid(object1.getLessonUid());
			break;
		case ApiConstants.LESSON:
			studentsClassActivity.setLessonUid(object1.getLessonUid());
			break;
		default:
			LOG.debug("Do nothing in collection/assessment level");
			break;
		}
		studentsClassActivity.setCollectionUid(object1.getCollectionUid());
		studentsClassActivity.setCollectionType(object1.getCollectionType());
		studentsClassActivity.setScore(sum(object1.getScore(), object2.getScore()));
		studentsClassActivity.setReaction(sum(object1.getReaction(), object2.getReaction()));
		studentsClassActivity.setViews(sum(object1.getViews(), object2.getViews()));
		studentsClassActivity.setTimeSpent(sum(object1.getTimeSpent(), object2.getTimeSpent()));
		return studentsClassActivity;
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
}
