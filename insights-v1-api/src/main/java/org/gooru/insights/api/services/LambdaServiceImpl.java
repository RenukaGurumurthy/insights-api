package org.gooru.insights.api.services;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.springframework.stereotype.Component;


@Component
public class LambdaServiceImpl implements LambdaService{

	public void aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList,  Integer depth1, Integer depth2) {
		long startTime =  System.currentTimeMillis();
		Map<Object, Map<Object, List<ContentTaxonomyActivity>>> data = resultList.stream().collect(Collectors.groupingBy(object -> ContentTaxonomyActivity.taxonomyDepthField(object,depth1), Collectors.groupingBy(object -> ContentTaxonomyActivity.taxonomyDepthField(object,depth2))));
		List<List<ContentTaxonomyActivity>> resultData = data.entrySet().stream().map(firstLevelObject -> firstLevelObject.getValue().entrySet().stream().map(secondLevelObject -> secondLevelObject.getValue().stream().reduce((f1,f2) -> {
			ContentTaxonomyActivity  contentTaxonomyActivity = new ContentTaxonomyActivity(f1);
			contentTaxonomyActivity.setScore(f1.getScore()+f2.getScore());
			contentTaxonomyActivity.setViews(f1.getViews()+f2.getViews());
			contentTaxonomyActivity.setTimespent(f1.getTimespent()+f2.getTimespent());
			return contentTaxonomyActivity;
		}).get()).collect(Collectors.toList())).collect(Collectors.toList());
		resultList.clear();
		Iterator<List<ContentTaxonomyActivity>> dataList = resultData.iterator();
		while(dataList.hasNext()) {
			resultList.addAll(dataList.next());
		}
		long endTime = System.currentTimeMillis();
		System.out.println(" content taxonomy activity 2nd level aggregation Time:"+(endTime - startTime));
	}
	
	public void aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList, Integer depth) {
		long startTime =  System.currentTimeMillis();
		List<ContentTaxonomyActivity> value = resultList.stream().collect(Collectors.groupingBy(object -> { return ContentTaxonomyActivity.taxonomyDepthField(object,depth); })).entrySet().stream().map(firstLevelObject -> firstLevelObject.getValue().stream().reduce((f1,f2) -> {
			ContentTaxonomyActivity  contentTaxonomyActivity = new ContentTaxonomyActivity(f1);
			contentTaxonomyActivity.setScore(f1.getScore()+f2.getScore());
			contentTaxonomyActivity.setViews(f1.getViews()+f2.getViews());
			contentTaxonomyActivity.setTimespent(f1.getTimespent()+f2.getTimespent());
			return contentTaxonomyActivity;
		})).map(f -> f.get()).collect(Collectors.toList());
		resultList.clear();
		resultList.addAll(value);
		long endTime = System.currentTimeMillis();
		System.out.println(" content taxonomy activity 1st level aggregation Time:"+(endTime - startTime));
	}
}
