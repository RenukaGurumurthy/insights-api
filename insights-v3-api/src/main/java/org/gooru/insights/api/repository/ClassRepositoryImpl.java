package org.gooru.insights.api.repository;

import java.util.List;
import java.util.Map;

import org.gooru.insights.api.daos.BaseRepositoryImpl;
import org.gooru.insights.api.models.InsightsConstant;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.type.StandardBasicTypes;
import org.springframework.stereotype.Repository;

@Repository
public class ClassRepositoryImpl extends BaseRepositoryImpl implements ClassRepository,InsightsConstant {

	private final String RETRIEVE_SESSIONS = "select session_activity_id,start_time,sequence from session_activity where parent_id =:parentId and collection_id =:collectionId";

	private final String RETRIEVE_CONTENT_ID = "SELECT content_id AS contentId from content WHERE gooru_oid =:gooruOid";
	
	private final String RETRIEVE_COLLECTION_DATA_BY_SESSION = "select r.title,r.description,r.category,r.resource_format_id as resourceFormatId,sa.views_in_session as views,sa.time_spent_in_millis as timespent,sa.rating,sa.reaction,sa.score from session_activity sa inner join resource r on sa.collection_id = r.content_id where session_activity_id =:sessionId and parent_id =:parentId and collection_id =:collectionId";
	
	private final String RETRIEVE_COLLECTION_DATA_BY_ALL_SESSION = "select r.title,r.description,r.category,r.resource_format_id as resourceFormatId,SUM(sa.views_in_session) as totalViews ,(SUM(sa.time_spent_in_millis)/count(sa.session_activity_id)) as avgTimesspent,(SUM(sa.rating)/count(sa.session_activity_id)) as avgRating,(SUM(sa.reaction)/count(sa.session_activity_id)) as avgReaction,(SUM(sa.score)/count(sa.session_activity_id)) as avgScore from session_activity sa inner join resource r on sa.collection_id = r.content_id where parent_id =:parentId and collection_id =:collectionId";
	
	private final String RETRIEVE_RESOURCE_DATA_BY_SESSION = "select r.title,r.thumbnail,r.description,r.category,r.resource_format_id as resourceFormatId,ctv.value as resourceFormat ,agg_data.resource_id as resourceId, agg_data.views_in_session as views,agg_data.time_spent_in_millis as timespent,agg_data.rating,agg_data.reaction,agg_data.score,agg_data.answer_status as answerStatus, agg_data.attempt_count as attemptCount,CAST(agg_data.question_type AS CHAR) as questionType,agg_data.answer_id as answerId,agg_data.answer_option_sequence as answerOptionSequence,agg_data.answer_text as answerText from collection_item ci inner join resource r on ci.resource_content_id = r.content_id inner join custom_table_value ctv on r.resource_format_id = ctv.custom_table_value_id left join (select resource_id,views_in_session,time_spent_in_millis,rating,reaction,score,attempt_count,question_type,answer_status,answer_id,answer_option_sequence,answer_text from session_activity_item where session_activity_id =:sessionId) as agg_data on agg_data.resource_id = r.content_id  where ci.collection_content_id =:collectionId";
	
	private final String RETRIEVE_RESOURCE_DATA_BY_ALL_SESSION = "select r.title,r.thumbnail,r.description,r.category,r.resource_format_id as resourceFormatId,ctv.value as resourceFormat ,agg_data.resource_id as resourceId, agg_data.views_in_session as totalViews,agg_data.avg_time_spent_in_millis as avgTimesspent,agg_data.avg_reaction as avgReaction, agg_data.attempt_count as totalAttemptCount,CAST(agg_data.question_type AS CHAR) as questionType from collection_item ci inner join resource r on ci.resource_content_id = r.content_id inner join custom_table_value ctv on r.resource_format_id = ctv.custom_table_value_id left join (select sai.resource_id,SUM(sai.views_in_session) as views_in_session ,SUM(sai.time_spent_in_millis)/SUM(sai.views_in_session) as avg_time_spent_in_millis,(SUM(sai.reaction)/count(1)) as avg_reaction ,SUM(sai.attempt_count) as attempt_count,sai.question_type from session_activity sa inner join session_activity_item sai on sa.session_activity_id = sai.session_activity_id where sa.parent_id =:parentId and sa.collection_id =:collectionId group by sai.resource_id ) as agg_data on agg_data.resource_id = r.content_id  where ci.collection_content_id =:collectionId";
	
	private final String RETRIEVE_LAST_SESSION_ACTIVITY_ID = "select session_activity_id AS sessionActivityId,sequence from session_activity where user_uid =:userUid and parent_id =:parentId and collection_id =:collectionId order by sequence desc LIMIT 1";
	
	private final String RETRIEVE_PROGRESS_REPORT_BY_FIRST_SESSION = "select u.username,r.title as resourceTitle,agg_data.session_activity_id as sessionActivityId, IFNULL(agg_data.views_in_session,0) AS Views ,IFNULL(agg_data.time_spent_in_millis,0) AS totalTimeSpentInMs ,IFNULL(agg_data.reaction,0) AS reaction,agg_data.question_type AS questionType,agg_data.answer_status AS answerStatus ,IFNULL(agg_data.score,0) as Score,agg_data.answer_option_sequence as answerOptionSequence,agg_data.answer_text as answerText from classpage cl inner join user_group ug on cl.classpage_code = ug.user_group_code inner join user_group_association uga on ug.user_group_uid = uga.user_group_uid inner join user u on u.gooru_uid = uga.gooru_uid inner join collection_item ci on ci.collection_content_id = cl.classpage_content_id and ci.resource_content_id =:collectionId inner join collection_item resource_item on resource_item.collection_content_id = ci.resource_content_id inner join resource r on resource_item.resource_content_id = r.content_id left join (select  sa.session_activity_id, sa.user_uid,sa.collection_id,sai.views_in_session ,sai.time_spent_in_millis ,sai.question_type,sai.reaction,sai.answer_status,sai.answer_option_sequence,sai.answer_text ,(sai.score) as score,sai.resource_id as resource_id from session_activity sa inner join session_activity_item sai on sa.session_activity_id = sai.session_activity_id where sa.parent_id =:parentId and sa.collection_id =:collectionId and sa.sequence = 1  group by sa.collection_id,sai.resource_id,sa.user_uid) as agg_data on agg_data.user_uid = u.gooru_uid and r.content_id = agg_data.resource_id where classpage_content_id =:parentId";

	private final String RETRIEVE_PROGRESS_REPORT_BY_RECENT_SESSION = "select u.username,r.title as resourceTitle,agg_data.session_activity_id as sessionActivityId, IFNULL(agg_data.views_in_session,0) AS Views ,IFNULL(agg_data.time_spent_in_millis,0) AS totalTimeSpentInMs ,IFNULL(agg_data.reaction,0) AS reaction,agg_data.question_type AS questionType,agg_data.answer_status AS answerStatus ,IFNULL(agg_data.score,0) as Score,agg_data.answer_option_sequence as answerOptionSequence,agg_data.answer_text as answerText from classpage cl inner join user_group ug on cl.classpage_code = ug.user_group_code inner join user_group_association uga on ug.user_group_uid = uga.user_group_uid inner join user u on u.gooru_uid = uga.gooru_uid inner join collection_item ci on ci.collection_content_id = cl.classpage_content_id and ci.resource_content_id =:collectionId inner join collection_item resource_item on resource_item.collection_content_id = ci.resource_content_id inner join resource r on resource_item.resource_content_id = r.content_id left join (select  sa.session_activity_id, sa.user_uid,sa.collection_id,sai.views_in_session ,sai.time_spent_in_millis ,sai.question_type,sai.reaction,sai.answer_status,sai.answer_option_sequence,sai.answer_text ,(sai.score) as score,sai.resource_id as resource_id from session_activity sa inner join session_activity_item sai on sa.session_activity_id = sai.session_activity_id where sa.session_activity_id =:sessionId   group by sa.collection_id,sai.resource_id,sa.user_uid) as agg_data on agg_data.user_uid = u.gooru_uid and r.content_id = agg_data.resource_id where classpage_content_id =:parentId";
	
	private final String RETRIEVE_ASSESSMENT_REPORT_BY_FIRST_SESSION = "select u.username,r.title as resourceTitle,agg_data.session_activity_id as sessionActivityId,IFNULL(agg_data.score,0) as score from classpage cl inner join user_group ug on cl.classpage_code = ug.user_group_code inner join user_group_association uga on ug.user_group_uid = uga.user_group_uid inner join user u on u.gooru_uid = uga.gooru_uid inner join collection_item ci on ci.collection_content_id = cl.classpage_content_id inner join resource r on ci.resource_content_id = r.content_id left join (select  session_activity_id, user_uid,collection_id,score from session_activity where parent_id =:parentId and sequence = 1) as agg_data on agg_data.user_uid = u.gooru_uid and r.content_id = agg_data.collection_id where classpage_content_id =:parentId;";


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
	public List<Map<String, Object>> getCollectionSessionData(long parentId, long collectionId, long sessionId) {
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_COLLECTION_DATA_BY_SESSION);
		query.setParameter(SESSION_ID, sessionId);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}
	
	@Override
	public List<Map<String, Object>> getCollectionAggregatedDataByAllSession(long parentId,long collectionId) {
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_COLLECTION_DATA_BY_ALL_SESSION);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}
	
	@Override
	public List<Map<String, Object>> getResourceSessionData(long parentId, long collectionId, long sessionId) {
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_RESOURCE_DATA_BY_SESSION);
		query.setParameter(SESSION_ID, sessionId);
		query.setParameter(COLLECTION_ID, collectionId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}

	@Override
	public List<Map<String, Object>> getResourceAggregatedDataByAllSession(long parentId, long collectionId) {
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_RESOURCE_DATA_BY_ALL_SESSION);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}
	
	@Override
	public Long getRecentSessionAcitivityId(long parentId, long collectionId,String userUid) {
		Session session = getSession();
		Query query = session.createSQLQuery(RETRIEVE_LAST_SESSION_ACTIVITY_ID);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		query.setParameter(USER_UID, userUid);
		List<Long> results = query.list();
		return (results != null && results.size() > 0) ? results.get(0) : 0L;
	}
	
	@Override
	public List<Map<String, Object>> getMastryReportsByRecentSessions(long collectionId,long parentId,long sessionActivityId,String reportType) {
		Session session = getSession();
		Query query = null;
		if(reportType.equalsIgnoreCase(PROGRESS)){
			query = session.createSQLQuery(RETRIEVE_PROGRESS_REPORT_BY_RECENT_SESSION);
			query.setParameter(COLLECTION_ID, collectionId);
		}else if(reportType.equalsIgnoreCase(ASSESSMENT)){
			query = session.createSQLQuery(RETRIEVE_PROGRESS_REPORT_BY_FIRST_SESSION);
		}
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(SESSION_ID, sessionActivityId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}

	@Override
	public List<Map<String, Object>> getMastryReportsByFirstSession(long collectionId,long parentId, String reportType) {
		Session session = getSession();
		Query query = null;
		if(reportType.equalsIgnoreCase(PROGRESS)){
			query = session.createSQLQuery(RETRIEVE_PROGRESS_REPORT_BY_FIRST_SESSION);
			query.setParameter(COLLECTION_ID, collectionId);
		}else if(reportType.equalsIgnoreCase(ASSESSMENT)){
			query = session.createSQLQuery(RETRIEVE_ASSESSMENT_REPORT_BY_FIRST_SESSION);
		}
		query.setParameter(PARENT_ID, parentId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}

}
