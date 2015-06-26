package org.gooru.insights.api.services;

import java.util.List;
import java.util.Map;

import org.gooru.insights.api.models.RequestParamDTO;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.models.SessionActivity;

public interface ClassService {

	ResponseParamDTO<Map<String, Object>> getSessions(RequestParamDTO requestParamDTO);
	ResponseParamDTO<Map<String, Object>> getCollectionSessionData(RequestParamDTO requestParamDTO);
	ResponseParamDTO<Map<String, Object>> getCollectionResourceSessionData(RequestParamDTO requestParamDTO);
	ResponseParamDTO<Map<String, Object>> getOEResponseData(RequestParamDTO requestParamDTO);
	ResponseParamDTO<Map<String, Object>> getMasteryReportDataForFirstSession(RequestParamDTO requestParamDTO);
	List<SessionActivity> getSessionActivity(Long id);

}
