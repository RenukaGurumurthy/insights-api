package org.gooru.insights.api.repository;

import java.util.List;

public interface ClassRepository {

	List<Object[]> getSession(long parentId, long collectionId, String userUid);

	Long getContentId(String contentGooruOid);

	List<Object[]> getCollectionAggregatedData(long parentId, long collectionId, String sessionId);

	List<Object[]> getResourceAggregatedData(long parentId, long collectionId, String sessionId);

}
