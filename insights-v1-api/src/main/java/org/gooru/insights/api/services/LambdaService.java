package org.gooru.insights.api.services;

import java.util.List;

import org.gooru.insights.api.models.ContentTaxonomyActivity;

public interface LambdaService {

	void aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList, Integer depth1, Integer depth2);

	void aggregateTaxonomyActivityData(List<ContentTaxonomyActivity> resultList, Integer depth);
}
