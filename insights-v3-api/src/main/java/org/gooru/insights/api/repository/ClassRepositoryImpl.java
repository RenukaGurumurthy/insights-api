package org.gooru.insights.api.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	
	private final String RETRIEVE_COLLECTION_DATA_BY_SESSION = "select r.title,r.description,r.category,r.resource_format_id,sa.views_in_session,sa.time_spent_in_millis,sa.rating,sa.reaction,sa.score from session_activity sa inner join resource r on sa.collection_id = r.content_id where session_activity_id =:sessionId and parent_id =:parentId and collection_id =:collectionId";
	
	private final String RETRIEVE_COLLECTION_DATA_BY_ALL_SESSION = "select r.title,r.description,r.category,r.resource_format_id,SUM(sa.views_in_session) as views_in_session ,(SUM(sa.time_spent_in_millis)/count(sa.session_activity_id)) as time_spent_in_millis,(SUM(sa.rating)/count(sa.session_activity_id)) as avg_rating,(SUM(sa.reaction)/count(sa.session_activity_id)) as avg_reaction,(SUM(sa.score)/count(sa.session_activity_id)) as avg_score, count(sa.session_activity_id) from session_activity sa inner join resource r on sa.collection_id = r.content_id where parent_id =:parentId and collection_id =:collectionId";
	
	private final String RETRIEVE_RESOURCE_DATA_BY_SESSION = "select r.title,r.thumbnail,r.description,r.category,r.resource_format_id,ctv.value as resource_format ,agg_data.resource_id, agg_data.views_in_session,agg_data.time_spent_in_millis,agg_data.rating,agg_data.reaction,agg_data.score,agg_data.answer_status, agg_data.attempt_count,CAST(agg_data.question_type AS CHAR),agg_data.answer_id,agg_data.answer_option_sequence,agg_data.answer_text from collection_item ci inner join resource r on ci.resource_content_id = r.content_id inner join custom_table_value ctv on r.resource_format_id = ctv.custom_table_value_id left join (select resource_id,views_in_session,time_spent_in_millis,rating,reaction,score,attempt_count,question_type,answer_status,answer_id,answer_option_sequence,answer_text from session_activity_item where session_activity_id =:sessionId) as agg_data on agg_data.resource_id = r.content_id  where ci.collection_content_id =:collectionId";
	
	private final String RETRIEVE_RESOURCE_DATA_BY_ALL_SESSION = "select r.title,r.thumbnail,r.description,r.category,r.resource_format_id,ctv.value as resource_format ,agg_data.resource_id, agg_data.views_in_session,agg_data.time_spent_in_millis,agg_data.avg_reaction, agg_data.attempt_count,CAST(agg_data.question_type AS CHAR) from collection_item ci inner join resource r on ci.resource_content_id = r.content_id inner join custom_table_value ctv on r.resource_format_id = ctv.custom_table_value_id left join (select sai.resource_id,SUM(sai.views_in_session) as views_in_session ,SUM(sai.time_spent_in_millis)/SUM(sai.views_in_session) as avg_time_spent_in_millis,(SUM(sai.reaction)/count(1)) as avg_reaction ,SUM(sai.attempt_count) as attempt_count,sai.question_type from session_activity sa inner join session_activity_item sai on sa.session_activity_id = sai.session_activity_id where sa.parent_id =:parentId and sa.collection_id =:collectionId group by sai.resource_id ) as agg_data on agg_data.resource_id = r.content_id  where ci.collection_content_id =:collectionId";
	
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
		List<Object[]> result = query.list().size() > 0 ? query.list() : null;
		return result;
	}
	
	@Override
	public List<Map<String, Object>> getCollectionSessionData(long parentId, long collectionId, long sessionId) {
		List<Map<String, Object>> resultMapAsList = new ArrayList<Map<String, Object>>();
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_COLLECTION_DATA_BY_SESSION);
		query.setParameter(SESSION_ID, sessionId);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		List<Object[]> resultObjectList = query.list().size() > 0 ? query.list() : null;
		if (resultObjectList != null) {
			for (Object[] resultObject : resultObjectList) {
				Map<String, Object> resultAsMap = new HashMap<String, Object>();
				resultAsMap.put(TITLE, resultObject[0]);
				resultAsMap.put(DESCRIPTION, resultObject[1]);
				resultAsMap.put(CATEGORY, resultObject[2]);
				resultAsMap.put(RESOURCE_FORMAT_ID, resultObject[3]);
				resultAsMap.put(VIEWS, resultObject[4]);
				resultAsMap.put(TIMESPENT, resultObject[5]);
				resultAsMap.put(RATING, resultObject[6]);
				resultAsMap.put(REACTION, resultObject[7]);
				resultAsMap.put(SCORE, resultObject[8]);
				resultMapAsList.add(resultAsMap);
			}
		}
		return resultMapAsList;
	}
	
	@Override
	public List<Map<String, Object>> getCollectionAggregatedDataByAllSession(long parentId,long collectionId) {
		List<Map<String, Object>> resultMapAsList = new ArrayList<Map<String, Object>>();
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_COLLECTION_DATA_BY_ALL_SESSION);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		List<Object[]> resultObjectList = query.list().size() > 0 ? query.list() : null;
		if (resultObjectList != null) {
			for (Object[] resultObject : resultObjectList) {
				Map<String, Object> resultAsMap = new HashMap<String, Object>();
				resultAsMap.put(TITLE, resultObject[0]);
				resultAsMap.put(DESCRIPTION, resultObject[1]);
				resultAsMap.put(CATEGORY, resultObject[2]);
				resultAsMap.put(RESOURCE_FORMAT_ID, resultObject[3]);
				resultAsMap.put(VIEWS, resultObject[4]);
				resultAsMap.put(AVG_TIMESPENT, resultObject[5]);
				resultAsMap.put(AVG_RATING, resultObject[6]);
				resultAsMap.put(AVG_REACTION, resultObject[7]);
				resultAsMap.put(AVG_SCORE, resultObject[8]);
				resultMapAsList.add(resultAsMap);
			}
		}
		return resultMapAsList;
	}
	
	@Override
	public List<Object[]> getResourceAggregatedData(long parentId, long collectionId, String sessionId) {		
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
	
	@Override
	public List<Map<String, Object>> getResourceSessionData(long parentId, long collectionId, long sessionId) {
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_RESOURCE_DATA_BY_SESSION);
		query.setParameter(SESSION_ID, sessionId);
		query.setParameter(COLLECTION_ID, collectionId);
		List<Object[]> resultObjectList = query.list().size() > 0 ? query.list() : null;
		List<Map<String, Object>> resultMapAsList = new ArrayList<Map<String, Object>>();
		if (resultObjectList != null) {
			for (Object[] resultObject : resultObjectList) {
				Map<String, Object> resultAsMap = new HashMap<String, Object>();
				resultAsMap.put(TITLE, resultObject[0]);
				resultAsMap.put(THUMBNAIL, resultObject[1]);
				resultAsMap.put(DESCRIPTION, resultObject[2]);
				resultAsMap.put(CATEGORY, resultObject[3]);
				resultAsMap.put(RESOURCE_FORMAT, resultObject[5]);
				resultAsMap.put(RESOURCE_ID, resultObject[6]);
				resultAsMap.put(VIEWS, resultObject[7]);
				resultAsMap.put(TIMESPENT, resultObject[8]);
				resultAsMap.put(RATING, resultObject[9]);
				resultAsMap.put(REACTION, resultObject[10]);
				resultAsMap.put(SCORE, resultObject[11]);
				resultAsMap.put(ANSWER_STATUS, resultObject[12]);
				resultAsMap.put(ATTEMPT_COUNT, resultObject[13]);
				resultAsMap.put(QUESTION_TYPE, resultObject[14]);
				resultAsMap.put(ANSWER_ID, resultObject[15]);
				resultAsMap.put(ANSWER_OPTION_SEQUENCE, resultObject[16]);
				resultAsMap.put(ANSWER_TEXT, resultObject[17]);
				resultMapAsList.add(resultAsMap);
			}
		}
		return resultMapAsList;
	}

	@Override
	public List<Map<String, Object>> getResourceAggregatedDataByAllSession(long parentId, long collectionId) {
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_RESOURCE_DATA_BY_ALL_SESSION);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		List<Object[]> resultObjectList = query.list().size() > 0 ? query.list() : null;
		List<Map<String, Object>> resultMapAsList = new ArrayList<Map<String, Object>>();
		if (resultObjectList != null) {
			for (Object[] resultObject : resultObjectList) {
				Map<String, Object> resultAsMap = new HashMap<String, Object>();
				resultAsMap.put(TITLE, resultObject[0]);
				resultAsMap.put(THUMBNAIL, resultObject[1]);
				resultAsMap.put(DESCRIPTION, resultObject[2]);
				resultAsMap.put(CATEGORY, resultObject[3]);
				resultAsMap.put(RESOURCE_FORMAT, resultObject[5]);
				resultAsMap.put(RESOURCE_ID, resultObject[6]);
				resultAsMap.put(TOTAL_VIEWS, resultObject[7]);
				resultAsMap.put(AVG_TIMESPENT, resultObject[8]);
				resultAsMap.put(TOTAL_REACTION, resultObject[9]);
				resultAsMap.put(TOTAL_ATTEMPT_COUNT, resultObject[10]);
				resultAsMap.put(QUESTION_TYPE, resultObject[11]);
				resultMapAsList.add(resultAsMap);
			}
		}
		return resultMapAsList;
	}
}
