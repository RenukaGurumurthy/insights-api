package org.gooru.insights.api.services;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.utils.InsightsLogger;
import org.springframework.stereotype.Component;


@Component
public class LambdaServiceImpl implements LambdaService{

	public void aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList,  Integer depth1, Integer depth2) {
		long startTime =  System.currentTimeMillis();
		
		Map<Object, Map<Object, List<ContentTaxonomyActivity>>> data = resultList.stream().collect(Collectors.groupingBy(object -> ContentTaxonomyActivity.taxonomyDepthField(object,depth1), Collectors.groupingBy(object -> ContentTaxonomyActivity.taxonomyDepthField((ContentTaxonomyActivity) object,depth2))));
		List<List<ContentTaxonomyActivity>> resultData = data.entrySet().stream().map(firstLevelObject -> firstLevelObject.getValue().entrySet().stream()
				.map(secondLevelObject -> secondLevelObject.getValue().stream().reduce((f1,f2) -> getContentTaxonomyActivity(f1,f2, depth2)).get()).collect(Collectors.toList())).collect(Collectors.toList());
		resultList.clear();
		Iterator<List<ContentTaxonomyActivity>> dataList = resultData.iterator();
		while(dataList.hasNext()) {
			resultList.addAll(dataList.next());
		}
		long endTime = System.currentTimeMillis();
		InsightsLogger.debug("content taxonomy activity 2nd level aggregation Time:"+(endTime - startTime));
	}
	
	public List<ContentTaxonomyActivity> aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> coreList, Integer depth) {

		long startTime =  System.currentTimeMillis();
		List<ContentTaxonomyActivity> questionActivity = filterTaxonomyActivity(coreList, ApiConstants.QUESTION);
		questionActivity = aggregateTaxonomyQuestionActivity(questionActivity, depth);
		List<ContentTaxonomyActivity>  resourceActivity = filterTaxonomyActivity(coreList, ApiConstants.RESOURCE);
		resourceActivity = aggregateTaxonomyResourceActivity(resourceActivity, depth);
		long endTime = System.currentTimeMillis();
		InsightsLogger.debug("content taxonomy activity 1st level aggregation Time:"+(endTime - startTime));
		return joinData(questionActivity, resourceActivity, depth);
	}
	
	private List<ContentTaxonomyActivity> joinData(List<ContentTaxonomyActivity> questionList, List<ContentTaxonomyActivity> resourceList, Integer depth) {
		
		for(ContentTaxonomyActivity question : questionList) {
			for(ContentTaxonomyActivity resource : resourceList) {
				if(ContentTaxonomyActivity.taxonomyDepthField(question, depth).equals(ContentTaxonomyActivity.taxonomyDepthField(resource, depth))) {
					question.setTimespent(resource.getTimespent());
				}
			}
		}
		if(questionList == null || questionList.isEmpty()) {
			return resourceList;
		}
		return questionList;
	}
	
	private List<ContentTaxonomyActivity> aggregateTaxonomyQuestionActivity(List<ContentTaxonomyActivity> resultList, Integer depth) {
	
		Map<Object, List<ContentTaxonomyActivity>> groupedData = aggregateActivity(resultList, depth);
		return groupedData.entrySet().stream()
				.map(firstLevelObject -> firstLevelObject.getValue().stream().reduce((f1,f2) -> generateTaxonomyQuestionMetrics(f1,f2, depth))).map(f -> f.get()).collect(Collectors.toList());
	}
	
	private List<ContentTaxonomyActivity> aggregateTaxonomyResourceActivity(List<ContentTaxonomyActivity> resultList, Integer depth) {
		
		Map<Object, List<ContentTaxonomyActivity>> groupedData = aggregateActivity(resultList, depth);
		return groupedData.entrySet().stream()
				.map(firstLevelObject -> firstLevelObject.getValue().stream().reduce((f1,f2) -> generateTaxonomyResourceMetrics(f1,f2, depth))).map(f -> f.get()).collect(Collectors.toList());
	}
	
	private List<ContentTaxonomyActivity> filterTaxonomyActivity(List<ContentTaxonomyActivity> resultList, String resourceType) {
		
		return resultList.stream().filter(data -> data.getResourceType().equalsIgnoreCase(resourceType)).collect(Collectors.toList());
	}
	
	private Map<Object, List<ContentTaxonomyActivity>> aggregateActivity(List<ContentTaxonomyActivity> resultList, Integer depth) {
	
		return resultList.stream().collect(Collectors.groupingBy(object -> { return ContentTaxonomyActivity.taxonomyDepthField(object,depth); }));
	}

	private ContentTaxonomyActivity generateTaxonomyQuestionMetrics(ContentTaxonomyActivity  object1, ContentTaxonomyActivity  object2, Integer depth) {

		ContentTaxonomyActivity  contentTaxonomyActivity = new ContentTaxonomyActivity(object1,depth);
		contentTaxonomyActivity.setScore(object1.getScore()+object2.getScore());
		contentTaxonomyActivity.setAttempts(object1.getAttempts()+object2.getAttempts());
		return contentTaxonomyActivity;
	}
	
	private ContentTaxonomyActivity generateTaxonomyResourceMetrics(ContentTaxonomyActivity  object1, ContentTaxonomyActivity  object2, Integer depth) {

		ContentTaxonomyActivity  contentTaxonomyActivity = new ContentTaxonomyActivity(object1,depth);
		contentTaxonomyActivity.setTimespent(object1.getTimespent()+object2.getTimespent());
		return contentTaxonomyActivity;
	}
	
	private ContentTaxonomyActivity getContentTaxonomyActivity(ContentTaxonomyActivity  object1, ContentTaxonomyActivity  object2, Integer depth) {
		ContentTaxonomyActivity  contentTaxonomyActivity = new ContentTaxonomyActivity(object1,depth);
		if(object1.getResourceType().equalsIgnoreCase(ApiConstants.QUESTION) && object2.getResourceType().equalsIgnoreCase(ApiConstants.QUESTION)) {

			contentTaxonomyActivity.setScore(object1.getScore()+object2.getScore());
			contentTaxonomyActivity.setAttempts(object1.getAttempts()+object2.getAttempts());
		} else if(object1.getResourceType().equalsIgnoreCase(ApiConstants.RESOURCE) && object2.getResourceType().equalsIgnoreCase(ApiConstants.RESOURCE)) {
			
			contentTaxonomyActivity.setTimespent(object1.getTimespent()+object2.getTimespent());
		} 
		return contentTaxonomyActivity;
	}
	

}
