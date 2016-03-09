package org.gooru.insights.api.daos;



import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.InsightsConstant;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

@Repository
public class CqlCassandraDaoImpl extends CassandraConnectionProvider implements CqlCassandraDao,InsightsConstant {

	//public static final String GET_SESSION_ACTIVITY_TYPE = "SELECT event_type FROM user_session_activity WHERE session_id = ? AND gooru_oid = ?";
	@Override
	public ResultSet getSessionActivityType(String sessionId, String gooruOid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._SESSION_ID, sessionId))
				.and(QueryBuilder.eq(ApiConstants._GOORU_OID, gooruOid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_COLLECTION_SESSIONS = "SELECT event_time,session_id,event_type FROM user_sessions WHERE user_uid = ? AND collection_uid = ? AND collection_type = ? AND class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ?";
	@Override
	public ResultSet getUserCollectionSessions(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSIONS.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, collectionUid))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_TYPE, collectionType))
				.and(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
				.and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseUid))
				.and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitUid))
				.and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonUid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_ASSESSMENT_SESSIONS = "SELECT event_time,session_id,event_type FROM user_sessions WHERE user_uid = ? AND collection_uid = ? AND collection_type = ? AND class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND event_type = ?";
	@Override
	public ResultSet getUserAssessmentSessions(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid, String eventType) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSIONS.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, collectionUid))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_TYPE, collectionType))
				.and(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
				.and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseUid))
				.and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitUid))
				.and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonUid))
				.and(QueryBuilder.eq(ApiConstants._EVENT_TYPE, eventType))
				;
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_SESSION_ACTIVITY = "SELECT * FROM user_session_activity WHERE session_id = ?";
	@Override
	public ResultSet getUserSessionActivity(String sessionId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._SESSION_ID, sessionId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_SESSION_CONTENT_ACTIVITY = "SELECT * FROM user_session_activity WHERE session_id = ? AND gooru_oid = ? ";
	@Override
	public ResultSet getUserSessionContentActivity(String sessionId, String gooruOid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._SESSION_ID, sessionId))
				.and(QueryBuilder.eq(ApiConstants._GOORU_OID, gooruOid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ? AND user_uid = ? ";
	@Override
	public ResultSet getUserCurrentLocationInClass(String classUid, String userUid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.STUDENT_LOCATION.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
				.and(QueryBuilder.eq(ApiConstants._USER_UID, userUid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_ALL_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ?";
	@Override
	public ResultSet getAllUserCurrentLocationInClass(String classUid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.STUDENT_LOCATION.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
				;
		return getCassSession().execute(select);
	}
	

	//public static final String GET_USER_CLASS_CONTENT_LATEST_SESSION = "SELECT session_id,user_uid FROM user_class_collection_last_sessions WHERE class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND collection_uid = ? AND user_uid = ?";
	@Override
	public ResultSet getUserClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid, String userUid) {
		Statement select = QueryBuilder
				.select()
				.all()
				.from(getLogKeyspaceName(),
						ColumnFamilySet.USER_CLASS_COLLECTION_LAST_SESSIONS.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
				.and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseUid))
				.and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitUid))
				.and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonUid))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, collectionUid))
				.and(QueryBuilder.eq(ApiConstants._USER_UID, userUid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USERS_CLASS_CONTENT_LATEST_SESSION = "SELECT session_id,user_uid FROM user_class_collection_last_sessions WHERE class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND collection_uid = ?";
	@Override
	public ResultSet getUsersClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid) {
		Statement select = QueryBuilder
				.select()
				.all()
				.from(getLogKeyspaceName(),
						ColumnFamilySet.USER_CLASS_COLLECTION_LAST_SESSIONS.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
				.and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseUid))
				.and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitUid))
				.and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonUid))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, collectionUid));
		return getCassSession().execute(select);
	}

	//public static final String GET_USER_CLASS_ACTIVITY_DATACUBE = "SELECT * FROM class_activity_datacube WHERE row_key = ? AND user_uid = ? AND collection_type = ?";
	@Override
	public ResultSet getUserClassActivityDatacube(String rowKey, String userUid, String collectionType) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._ROW_KEY, rowKey))
				.and(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_TYPE, collectionType));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_CLASS_ACTIVITY_DATACUBE = "SELECT * FROM class_activity_datacube WHERE row_key = ? AND collection_type = ?";
	@Override
	public ResultSet getClassActivityDatacube(String sessionId, String collectionType) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._ROW_KEY, sessionId))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_TYPE, collectionType));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_PEER_DETAIL = "SELECT * FROM class_activity_peer_detail WHERE row_key = ?";
	@Override
	public ResultSet getUserPeerDetail(String rowKey) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.CLASS_ACTIVITY_PEER_DETAIL.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._ROW_KEY, rowKey))
				;
		return getCassSession().execute(select);
	}
	
	//Taxonomy Query's
	
	//public static final String GET_SUBJECT_ACTIVITY = " SELECT course_id,domain_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =?";
	@Override
	public ResultSet getSubjectActivity(String rowKey, String subjectId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._USER_UID, rowKey))
				.and(QueryBuilder.eq(ApiConstants._SUBJECT_ID, subjectId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_COURSE_ACTIVITY = " SELECT domain_id,standards_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ?";
	@Override
	public ResultSet getCourseActivity(String rowKey, String subjectId,
			String courseId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._USER_UID, rowKey))
				.and(QueryBuilder.eq(ApiConstants._SUBJECT_ID, subjectId))
				.and(QueryBuilder.eq(ApiConstants._COURSE_ID, courseId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_DOMAIN_ACTIVITY = " SELECT standards_id,learning_targets_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ? AND domain_id = ?";
	@Override
	public ResultSet getDomainActivity(String rowKey, String subjectId,
			String courseId, String domainId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._USER_UID, rowKey))
				.and(QueryBuilder.eq(ApiConstants._SUBJECT_ID, subjectId))
				.and(QueryBuilder.eq(ApiConstants._COURSE_ID, courseId))
				.and(QueryBuilder.eq(ApiConstants._DOMAIN_ID, courseId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_STANDARDS_ACTIVITY = " SELECT learning_targets_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ? AND domain_id = ? AND standards_id = ?";
	@Override
	public ResultSet getStandardsActivity(String rowKey, String subjectId,
			String courseId, String domainId, String standardsId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._USER_UID, rowKey))
				.and(QueryBuilder.eq(ApiConstants._SUBJECT_ID, subjectId))
				.and(QueryBuilder.eq(ApiConstants._COURSE_ID, courseId))
				.and(QueryBuilder.eq(ApiConstants._DOMAIN_ID, domainId))
				.and(QueryBuilder.eq(ApiConstants._STANDARDS_ID, standardsId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_STUDENT_QUESTION_GRADE = "SELECT question_id,score FROM student_question_grade WHERE teacher_uid = ? AND user_uid = ? AND session_id = ?";
	@Override
	public ResultSet getStudentQuestionGrade(String teacherUid, String userUid,
			String sessionId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(),ColumnFamilySet.STUDENT_QUESTION_GRADE.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._TEACHER_UID, teacherUid))
				.and(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
				.and(QueryBuilder.eq(ApiConstants._SESSION_ID, sessionId));
		return getCassSession().execute(select);
	}
}
