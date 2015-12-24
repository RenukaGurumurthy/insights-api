package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface ItemService {

	
	ResponseParamDTO<Map<String, Object>> getItemDetail(String fields,String itemIds,String startDate,String endDate,String format,String datelevel,String granularity) throws Exception;
		
	ResponseParamDTO<Map<String, Object>> getMetadataDetails() throws Exception;
}
