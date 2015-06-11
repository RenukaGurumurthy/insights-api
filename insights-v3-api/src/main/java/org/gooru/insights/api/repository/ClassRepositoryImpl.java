package org.gooru.insights.api.repository;

import java.util.ArrayList;
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

	private final String FETCH_SESSIONS = "SELECT session_activity_id, start_time, sequence FROM session_activity WHERE parent_id =:parentId AND collection_id =:collectionId";

	private final String FETCH_CONTENT_ID = "SELECT content_id AS contentId FROM content WHERE gooru_oid =:gooruOid";
	
	private final String FETCH_COLLECTION_DATA_BY_SESSION = "SELECT r.title, r.description, r.category, r.resource_format_id AS resourceFormatId, sa.views_in_session AS views, sa.time_spent_in_millis AS timespent, sa.rating, sa.reaction, sa.score FROM session_activity sa INNER JOIN resource r ON sa.collection_id = r.content_id WHERE session_activity_id =:sessionId AND parent_id =:parentId AND collection_id =:collectionId";
	
	private final String FETCH_COLLECTION_DATA_BY_ALL_SESSION = "SELECT r.title, r.description, r.category, r.resource_format_id AS resourceFormatId, SUM(sa.views_in_session) AS totalViews ,(SUM(sa.time_spent_in_millis)/COUNT(sa.session_activity_id)) AS avgTimesspent, (SUM(sa.rating)/COUNT(sa.session_activity_id)) as avgRating, (SUM(sa.reaction)/COUNT(sa.session_activity_id)) AS avgReaction,(SUM(sa.score)/COUNT(sa.session_activity_id)) AS avgScore FROM session_activity sa INNER JOIN resource r ON sa.collection_id = r.content_id WHERE parent_id =:parentId AND collection_id =:collectionId";
	
	private final String FETCH_RESOURCE_DATA_BY_SESSION = "SELECT r.title, r.thumbnail, r.description, r.category, r.resource_format_id AS resourceFormatId, ctv.value AS resourceFormat ,agg_data.resource_id AS resourceId, agg_data.views_in_session AS views, agg_data.time_spent_in_millis AS timespent, agg_data.rating, agg_data.reaction, agg_data.score, agg_data.answer_status AS answerStatus, agg_data.attempt_count AS attemptCount, CAST(agg_data.question_type AS CHAR) AS questionType, agg_data.answer_id AS answerId, agg_data.answer_option_sequence AS answerOptionSequence, agg_data.answer_text AS answerText FROM collection_item ci INNER JOIN resource r ON ci.resource_content_id = r.content_id INNER JOIN custom_table_value ctv ON r.resource_format_id = ctv.custom_table_value_id LEFT JOIN (select resource_id, views_in_session, time_spent_in_millis, rating, reaction, score, attempt_count, question_type, answer_status, answer_id, answer_option_sequence, answer_text FROM session_activity_item WHERE session_activity_id =:sessionId) AS agg_data ON agg_data.resource_id = r.content_id  WHERE ci.collection_content_id =:collectionId";
	
	private final String FETCH_RESOURCE_DATA_BY_ALL_SESSION = "SELECT r.title, r.thumbnail, r.description, r.category, r.resource_format_id AS resourceFormatId, ctv.value AS resourceFormat ,agg_data.resource_id AS resourceId, agg_data.views_in_session AS totalViews, agg_data.avg_time_spent_in_millis AS avgTimesspent,agg_data.avg_reaction AS avgReaction, agg_data.attempt_count AS totalAttemptCount, CAST(agg_data.question_type AS CHAR) AS questionType FROM collection_item ci INNER JOIN resource r ON ci.resource_content_id = r.content_id INNER JOIN custom_table_value ctv ON r.resource_format_id = ctv.custom_table_value_id LEFT JOIN (SELECT sai.resource_id, SUM(sai.views_in_session) AS views_in_session, SUM(sai.time_spent_in_millis)/SUM(sai.views_in_session) AS avg_time_spent_in_millis, (SUM(sai.reaction)/count(1)) AS avg_reaction, SUM(sai.attempt_count) AS attempt_count, sai.question_type FROM session_activity sa INNER JOIN session_activity_item sai ON sa.session_activity_id = sai.session_activity_id WHERE sa.parent_id =:parentId AND sa.collection_id =:collectionId GROUP BY sai.resource_id) AS agg_data ON agg_data.resource_id = r.content_id  WHERE ci.collection_content_id =:collectionId";
	
	private final String FETCH_OE_RESPONSE_BY_SESSION = "SELECT u.username, CAST(sa.user_uid AS CHAR) AS gooruUId, CAST(sai.feedback_provided_user_uid AS CHAR) AS feedbackProviderUId, feedback_provided_time AS feedbackTimestamp, feedback_text AS feedbackText, answer_text AS OEText FROM session_activity AS sa INNER JOIN session_activity_item sai ON sai.session_activity_id = sa.session_activity_id INNER JOIN user u ON u.gooru_uid = sa.user_uid WHERE sai.session_activity_id =:sessionId AND sai.resource_id =:resourceId AND sa.parent_id =:parentId AND sa.collection_id =:collectionId";

	private final String FETCH_SESSION_USERS = "SELECT distinct CAST(sa.user_uid AS CHAR) AS gooruUId FROM session_activity AS sa WHERE sa.parent_id =:parentId AND sa.collection_id =:collectionId";
	
	private final String FETCH_RECENT_OE_RESPONSE_BY_USER = "SELECT u.username, CAST(sa.user_uid AS CHAR) AS gooruUId, CAST(sai.feedback_provided_user_uid AS CHAR) AS feedbackProviderUId, feedback_provided_time AS feedbackTimestamp, feedback_text AS feedbackText, answer_text AS OEText FROM session_activity sa INNER JOIN  session_activity_item sai ON sai.session_activity_id = sa.session_activity_id INNER JOIN user u ON u.gooru_uid = sa.user_uid WHERE sai.resource_id =:resourceId AND sa.parent_id =:parentId AND sa.collection_id =:collectionId AND sai.question_type = 'OE' AND sa.user_uid =:userUid ORDER BY sa.sequence DESC LIMIT 1";
	
	private final String RETRIEVE_LAST_SESSION_ACTIVITY_ID = "select session_activity_id AS sessionActivityId,sequence from session_activity where user_uid =:userUid and parent_id =:parentId and collection_id =:collectionId order by sequence desc LIMIT 1";
	
	private final String RETRIEVE_PROGRESS_REPORT_BY_FIRST_SESSION = "select u.username,r.title as resourceTitle,agg_data.session_activity_id as sessionActivityId, IFNULL(agg_data.views_in_session,0) AS Views ,IFNULL(agg_data.time_spent_in_millis,0) AS totalTimeSpentInMs ,IFNULL(agg_data.reaction,0) AS reaction,agg_data.question_type AS questionType,agg_data.answer_status AS answerStatus ,IFNULL(agg_data.score,0) as Score,agg_data.answer_option_sequence as answerOptionSequence,agg_data.answer_text as answerText from classpage cl inner join user_group ug on cl.classpage_code = ug.user_group_code inner join user_group_association uga on ug.user_group_uid = uga.user_group_uid inner join user u on u.gooru_uid = uga.gooru_uid inner join collection_item ci on ci.collection_content_id = cl.classpage_content_id and ci.resource_content_id =:collectionId inner join collection_item resource_item on resource_item.collection_content_id = ci.resource_content_id inner join resource r on resource_item.resource_content_id = r.content_id left join (select  sa.session_activity_id, sa.user_uid,sa.collection_id,sai.views_in_session ,sai.time_spent_in_millis ,sai.question_type,sai.reaction,sai.answer_status,sai.answer_option_sequence,sai.answer_text ,(sai.score) as score,sai.resource_id as resource_id from session_activity sa inner join session_activity_item sai on sa.session_activity_id = sai.session_activity_id where sa.parent_id =:parentId and sa.collection_id =:collectionId and sa.sequence = 1  group by sa.collection_id,sai.resource_id,sa.user_uid) as agg_data on agg_data.user_uid = u.gooru_uid and r.content_id = agg_data.resource_id where classpage_content_id =:parentId and uga.is_group_owner = 0";

	private final String RETRIEVE_PROGRESS_REPORT_BY_RECENT_SESSION = "select u.username,r.title as resourceTitle,agg_data.session_activity_id as sessionActivityId, IFNULL(agg_data.views_in_session,0) AS Views ,IFNULL(agg_data.time_spent_in_millis,0) AS totalTimeSpentInMs ,IFNULL(agg_data.reaction,0) AS reaction,agg_data.question_type AS questionType,agg_data.answer_status AS answerStatus ,IFNULL(agg_data.score,0) as Score,agg_data.answer_option_sequence as answerOptionSequence,agg_data.answer_text as answerText from classpage cl inner join user_group ug on cl.classpage_code = ug.user_group_code inner join user_group_association uga on ug.user_group_uid = uga.user_group_uid inner join user u on u.gooru_uid = uga.gooru_uid inner join collection_item ci on ci.collection_content_id = cl.classpage_content_id and ci.resource_content_id =:collectionId inner join collection_item resource_item on resource_item.collection_content_id = ci.resource_content_id inner join resource r on resource_item.resource_content_id = r.content_id left join (select  sa.session_activity_id, sa.user_uid,sa.collection_id,sai.views_in_session ,sai.time_spent_in_millis ,sai.question_type,sai.reaction,sai.answer_status,sai.answer_option_sequence,sai.answer_text ,(sai.score) as score,sai.resource_id as resource_id from session_activity sa inner join session_activity_item sai on sa.session_activity_id = sai.session_activity_id where sa.session_activity_id =:sessionId   group by sa.collection_id,sai.resource_id,sa.user_uid) as agg_data on agg_data.user_uid = u.gooru_uid and r.content_id = agg_data.resource_id where classpage_content_id =:parentId";
	
	private final String RETRIEVE_ASSESSMENT_REPORT_BY_FIRST_SESSION = "select u.username,r.title as resourceTitle,agg_data.session_activity_id as sessionActivityId,IFNULL(agg_data.score,0) as score from classpage cl inner join user_group ug on cl.classpage_code = ug.user_group_code inner join user_group_association uga on ug.user_group_uid = uga.user_group_uid inner join user u on u.gooru_uid = uga.gooru_uid inner join collection_item ci on ci.collection_content_id = cl.classpage_content_id inner join resource r on ci.resource_content_id = r.content_id left join (select  session_activity_id, user_uid,collection_id,score from session_activity where parent_id =:parentId and sequence = 1) as agg_data on agg_data.user_uid = u.gooru_uid and r.content_id = agg_data.collection_id where classpage_content_id =:parentId;";


	@Override
	public List<Object[]> getSession(long parentId,long collectionId,String userUid) {
		Session session = getSession();
		Query query = session.createSQLQuery(FETCH_SESSIONS);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		List<Object[]> result = query.list();
		return result;

	}
	
	@Override
	public Long getContentId(String contentGooruOid) {
		Session session = getSession();
		Query query = session.createSQLQuery(FETCH_CONTENT_ID).addScalar(CONTENTID, StandardBasicTypes.LONG);
		query.setParameter(GOORUOID, contentGooruOid);
		List<Long> results = query.list();
		return (results != null && results.size() > 0) ? results.get(0) : 0L;
	}
	
	@Override
	public List<Map<String, Object>> getCollectionSessionData(long parentId, long collectionId, long sessionId) {
		Session session = getSession();
		Query query = session.createSQLQuery(FETCH_COLLECTION_DATA_BY_SESSION);
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
		Query query = session.createSQLQuery(FETCH_COLLECTION_DATA_BY_ALL_SESSION);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}
	
	@Override
	public List<Map<String, Object>> getResourceSessionData(long parentId, long collectionId, long sessionId) {
		Session session = getSession();
		Query query = session.createSQLQuery(FETCH_RESOURCE_DATA_BY_SESSION);
		query.setParameter(SESSION_ID, sessionId);
		query.setParameter(COLLECTION_ID, collectionId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}

	@Override
	public List<Map<String, Object>> getResourceAggregatedDataByAllSession(long parentId, long collectionId) {
		Session session = getSession();
		Query query = session.createSQLQuery(FETCH_RESOURCE_DATA_BY_ALL_SESSION);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}
	
	@Override
	public List<Map<String, Object>> getOEResponseBySession(long parentId, long collectionId, long resourceId, Long sessionId) {
		List<Map<String, Object>> resultMapAsList = new ArrayList<Map<String, Object>>();
		Session session = getSession();
		Query query = session.createSQLQuery(FETCH_OE_RESPONSE_BY_SESSION);
		query.setParameter(PARENT_ID, parentId);
		query.setParameter(COLLECTION_ID, collectionId);
		query.setParameter(RESOURCE_ID, resourceId);
		query.setParameter(SESSION_ID, sessionId);
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}
	
	@Override
	public List<Map<String, Object>> getOEResponseByUser(long parentId, long collectionId, long resourceId, List<Object[]> userList) {
		List<Map<String, Object>> resultMapAsList = null;
		Session session = getSession();
		if (userList != null && userList.size() > 0) {
			resultMapAsList = new ArrayList<Map<String, Object>>();
			for (Object user : userList) {
				Query query = session.createSQLQuery(FETCH_RECENT_OE_RESPONSE_BY_USER);
				query.setParameter(PARENT_ID, parentId);
				query.setParameter(COLLECTION_ID, collectionId);
				query.setParameter(RESOURCE_ID, resourceId);
				query.setParameter(USER_UID, user);
				query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
				List<Map<String, Object>> resultMapList = query.list().size() > 0 ? query.list() : null;
				if (resultMapList != null && resultMapList.size() > 0) {
					resultMapAsList.add(resultMapList.get(0));
				}
			}
		}
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
		query.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY); 
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}

	@Override
	public List<Map<String, Object>> getMasteryReportsByFirstSession(long collectionId,long parentId, String reportType) {
		Session session = getSession();
		Query query = null;
		if(reportType.equalsIgnoreCase(PROGRESS)){
			query = session.createSQLQuery(RETRIEVE_PROGRESS_REPORT_BY_FIRST_SESSION);
			query.setParameter(COLLECTION_ID, collectionId);
		}else if(reportType.equalsIgnoreCase(ASSESSMENT)){
			query = session.createSQLQuery(RETRIEVE_ASSESSMENT_REPORT_BY_FIRST_SESSION);
		}
		query.setParameter(PARENT_ID, parentId);
		query.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY); 
		query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
		List<Map<String, Object>> resultMapAsList = query.list().size() > 0 ? query.list() : null;
		return resultMapAsList;
	}

	@Override
	public List<Object[]> fetchSessionActivityUserList(long parentId, long collectionId) {
        Session session = getSession();
        Query query = session.createSQLQuery(FETCH_SESSION_USERS);
        query.setParameter(PARENT_ID, parentId);
        query.setParameter(COLLECTION_ID, collectionId);
        List<Object[]> userList = query.list().size() > 0 ? query.list() : null;
        return userList;
	}
	
}
