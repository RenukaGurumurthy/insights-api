package org.gooru.insights.api.repository;

import java.util.List;
import java.util.Map;

public interface ClassRepository {

	List<Object[]> getSession(long parentId, long collectionId, String userUid);

	Long getContentId(String contentGooruOid);
	
	List<Map<String, Object>> getCollectionAggregatedDataByAllSession(long parentId,long collectionId);
	
	List<Map<String, Object>> getCollectionSessionData(long parentId,long collectionId,long sessionId);
	
	List<Map<String, Object>> getResourceSessionData(long parentId, long collectionId, long sessionId);		

	List<Map<String, Object>> getResourceAggregatedDataByAllSession(long parentId, long collectionId);
		
	List<Map<String, Object>> getOEQuestionData(long parentId, long collectionId, long resourceId, Long sessionId, List<Object[]> userList);

	Long getRecentSessionAcitivityId(long parentId, long collectionId,String userUid);

	List<Map<String, Object>> getMastryReportsByRecentSessions(long collectionId,long classId,long sessionActivityId,String reportType);

	List<Map<String, Object>> getMastryReportsByFirstSession(long collectionId,long classId,String reportType);
	
	List<Object[]> fetchSessionUsers(long parentId, long collectionId);
}
