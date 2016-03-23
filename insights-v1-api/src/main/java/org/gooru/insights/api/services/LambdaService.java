package org.gooru.insights.api.services;

import java.util.List;

import org.gooru.insights.api.models.ContentTaxonomyActivity;

public interface LambdaService {

	List<ContentTaxonomyActivity> aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList, Integer depth);
}
