package org.gooru.insights.api.services;

import java.util.List;
import java.util.Map;

import org.gooru.insights.api.models.RequestParamDTO;

public interface ClassService {

	String getTitle(Integer contentId);

	List<Map<String, Object>> getSessions(RequestParamDTO requestParamDTO);
}
