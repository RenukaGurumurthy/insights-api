package org.gooru.insights.api.services;

import java.util.List;

import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.StudentsClassActivity;

public interface LambdaService {

	List<ContentTaxonomyActivity> aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList, Integer depth);

	List<List<StudentsClassActivity>> aggregateStudentsClassActivityData(List<StudentsClassActivity> resultList,
			String aggregateLevel);

	List<StudentsClassActivity> applyFiltersInStudentsClassActivity(List<StudentsClassActivity> resultList,
			String collectionType);
}
