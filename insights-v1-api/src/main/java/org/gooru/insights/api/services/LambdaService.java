package org.gooru.insights.api.services;

import java.util.List;
import java.util.Map;

import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.SessionTaxonomyActivity;
import org.gooru.insights.api.models.StudentsClassActivity;

public interface LambdaService {

	List<ContentTaxonomyActivity> aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList, Integer depth);

	List<Map<String, List<StudentsClassActivity>>> aggregateStudentsClassActivityData(List<StudentsClassActivity> resultList,
			String collectionType, String aggregateLevel);
	
	List<SessionTaxonomyActivity> aggregateSessionTaxonomyActivity(List<SessionTaxonomyActivity> sessionTaxonomyActivity, String levelType);

	List<SessionTaxonomyActivity> aggregateSessionTaxonomyActivityByGooruOid(
			List<SessionTaxonomyActivity> sessionTaxonomyActivity);

	List<StudentsClassActivity> applyFiltersInStudentsClassActivity(List<StudentsClassActivity> resultList,
			String collectionType, String userUid);
}
