package org.gooru.insights.api.services;

import java.util.List;
import java.util.Map;

import org.gooru.insights.api.models.RequestParamDTO;

public interface ClassService {

	List<Map<String, Object>> getSessions(RequestParamDTO requestParamDTO);
	List<Map<String, Object>> getCollectionSessionData(RequestParamDTO requestParamDTO);
	List<Map<String, Object>> getCollectionResourceSessionData(RequestParamDTO requestParamDTO);

}
