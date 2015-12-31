package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.ResponseParamDTO;

public interface ClassV2Service {
	
	ResponseParamDTO<Map<String, Object>> getUserCurrentLocationInLesson(String userUid, String classId);

	ResponseParamDTO<Map<String, Object>> getUserPeers(String classId, String courseId, String unitId, String lessonId);	
}


