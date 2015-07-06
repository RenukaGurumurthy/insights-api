package org.gooru.insights.api.repository;

import java.util.List;
import java.util.Map;

public interface ClassRepository {

	List<Map<String, Object>> getSession(long parentId, long collectionId, String userUid);

	Long getContentId(String contentGooruOid);
	
	List<Map<String, Object>> getCollectionAggregatedDataByAllSession(long parentId,long collectionId);
	
	List<Map<String, Object>> getCollectionSessionData(long parentId,long collectionId,long sessionId);
	
	List<Map<String, Object>> getResourceSessionData(long parentId, long collectionId, long sessionId);		

	List<Map<String, Object>> getResourceAggregatedDataByAllSession(long parentId, long collectionId);
		
	Long getRecentSessionAcitivityId(long parentId, long collectionId,String userUid);

	List<Map<String, Object>> getMastryReportsByRecentSessions(long collectionId,long classId,long sessionActivityId,String reportType);

	List<Map<String, Object>> getMasteryReportsByFirstSession(long collectionId,long classId,String reportType);
	
	List<Object[]> fetchSessionActivityUserList(long parentId, long collectionId);
	
	List<Map<String, Object>> getOEResponseBySession(long parentId, long collectionId, long resourceId, Long sessionId);
	
	List<Map<String, Object>> getOEResponseByUser(long parentId, long collectionId, long resourceId, List<Object[]> userList);

}
