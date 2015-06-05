package org.gooru.insights.api.repository;

import java.util.List;

import org.apache.commons.lang.StringUtils;
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
	
	private final String RETRIEVE_COLLECTION_DATA_BY_SESSION = "select r.title,r.description,r.category,r.resource_format_id,sa.views_in_session,sa.time_spent_in_millis,sa.rating,sa.reaction,sa.score from session_activity sa inner join resource r on sa.collection_id = r.content_id where session_activity_id =:sessionId and parent_id =:classId and collection_id =:collectionId";
	
	private final String RETRIEVE_COLLECTION_DATA_BY_ALL_SESSION = "select count(1), r.title,r.description,r.category,r.resource_format_id,SUM(sa.views_in_session) as views_in_session ,(SUM(sa.time_spent_in_millis)/count(1)) as time_spent_in_millis,(SUM(sa.rating)/count(1)) as avg_rating,(SUM(sa.reaction)/count(1)) as avg_reaction,(SUM(sa.score)/count(1)) as avg_score from session_activity sa inner join resource r on sa.collection_id = r.content_id where parent_id =:classId and collection_id =:collectionId";
	
	private final String RETRIEVE_RESOURCE_DATA_BY_SESSION = "select r.title,r.thumbnail,r.description,r.category,r.resource_format_id,ctv.value as resource_format ,agg_data.resource_id, agg_data.views_in_session,agg_data.time_spent_in_millis,agg_data.rating,agg_data.reaction,agg_data.score,agg_data.answer_status, agg_data.attempt_count,agg_data.question_type,agg_data.answer_id,agg_data.answer_option_sequence,agg_data.answer_text from collection_item ci inner join resource r on ci.resource_content_id = r.content_id inner join custom_table_value ctv on r.resource_format_id = ctv.custom_table_value_id left join (select resource_id,views_in_session,time_spent_in_millis,rating,reaction,score,attempt_count,question_type,answer_status,answer_id,answer_option_sequence,answer_text from session_activity_item where session_activity_id =:sessionId) as agg_data on agg_data.resource_id = r.content_id  where ci.collection_content_id =:collectionId";
	
	private final String RETRIEVE_RESOURCE_DATA_BY_ALL_SESSION = "select r.title,r.thumbnail,r.description,r.category,r.resource_format_id,ctv.value as resource_format ,agg_data.resource_id, agg_data.views_in_session,agg_data.time_spent_in_millis,agg_data.avg_reaction, agg_data.attempt_count,agg_data.question_type from collection_item ci inner join resource r on ci.resource_content_id = r.content_id inner join custom_table_value ctv on r.resource_format_id = ctv.custom_table_value_id left join (select sai.resource_id,SUM(sai.views_in_session) as views_in_session ,(sai.time_spent_in_millis) as time_spent_in_millis,(SUM(sai.reaction)/count(1)) as avg_reaction ,SUM(sai.attempt_count) as attempt_count,sai.question_type from session_activity sa inner join session_activity_item sai on sa.session_activity_id = sai.session_activity_id where sa.parent_id =:classId and sa.collection_id =:collectionId group by sai.resource_id ) as agg_data on agg_data.resource_id = r.content_id  where ci.collection_content_id =:collectionId";
	
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
	
	@Override
	public List<Object[]> getCollectionAggregatedData(long parentId,long collectionId,String sessionId) {		
		Session session = getSession();
		Query query = null;
		if(StringUtils.isBlank(sessionId)|| sessionId.equalsIgnoreCase("0")){
			query = session.createSQLQuery(RETRIEVE_COLLECTION_DATA_BY_ALL_SESSION);
		}else{
			query = session.createSQLQuery(RETRIEVE_COLLECTION_DATA_BY_SESSION);
			query.setParameter(SESSION_ID, sessionId);
		}
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		List<Object[]> result = query.list();
		return result;
	}
	
	@Override
	public List<Object[]> getResourceAggregatedData(long parentId,long collectionId,String sessionId) {		
		Session session = getSession();
		Query query = null;
		if(StringUtils.isBlank(sessionId)|| sessionId.equalsIgnoreCase("0")){
			query = session.createSQLQuery(RETRIEVE_RESOURCE_DATA_BY_ALL_SESSION);
		}else{
			query = session.createSQLQuery(RETRIEVE_RESOURCE_DATA_BY_SESSION);
			query.setParameter(SESSION_ID, sessionId);
		}
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		List<Object[]> result = query.list();
		return result;
	}
}
