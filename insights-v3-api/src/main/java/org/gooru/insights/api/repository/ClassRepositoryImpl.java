package org.gooru.insights.api.repository;

import java.util.List;

import org.gooru.insights.api.daos.BaseRepositoryImpl;
import org.gooru.insights.api.models.InsightsConstant;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.type.StandardBasicTypes;
import org.springframework.stereotype.Repository;

@Repository
public class ClassRepositoryImpl extends BaseRepositoryImpl implements ClassRepository,InsightsConstant {

	private final String RETRIEVE_SESSIONS = "select session_activity_id,start_time,sequence from session_activity where parent_id =:parentId and collection_id =:collectionId";

	private final String RETRIEVE_CONTENT_ID = "SELECT content_id AS contentId from content WHERE gooru_oid =:gooruOid";
	
	@Override
	public List<Object[]> getSession(long parentId,long collectionId,String userUid) {
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_SESSIONS);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		List<Object[]> result = query.list();
		return result;

	}
	
	@Override
	public Long getContentId(String contentGooruOid) {
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_CONTENT_ID).addScalar("contentId", StandardBasicTypes.LONG);
		query.setParameter("gooruOid", contentGooruOid);
		List<Long> results = query.list();

		return (results != null && results.size() > 0) ? results.get(0) : 0L;
	}
	
}
