package org.gooru.insights.api.daos;



import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.utils.InsightsLogger;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.PreparedCqlQuery;
import com.netflix.astyanax.serializers.StringSerializer;

@Repository
public class CqlCassandraDaoImpl extends CassandraConnectionProvider implements CqlCassandraDao {

	private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;

	public CqlResult<String, String> executeCqlRowsQuery(String columnFamilyName, String query, String... parameters) {
	
		OperationResult<CqlResult<String, String>> result = null;
		try {
			PreparedCqlQuery<String, String> cqlQuery = getColumnFamilyQuery(columnFamilyName).withCql(query).asPreparedStatement();
			cqlQuery = setParameters(cqlQuery, parameters);
			result = cqlQuery.execute();
		} catch (ConnectionException e) {
			InsightsLogger.error("CQL Exception:"+query, e);
		}
		return result != null ? result.getResult() : null;	
	}
	
	public ColumnList<String> executeCqlRowQuery(String columnFamilyName, String query, String... parameters) {
		
		OperationResult<CqlResult<String, String>> result = null;
		try {
			PreparedCqlQuery<String, String> cqlQuery = getColumnFamilyQuery(columnFamilyName).withCql(query).asPreparedStatement();
			cqlQuery = setParameters(cqlQuery, parameters);
			result = cqlQuery.execute();
		} catch (ConnectionException e) {
			InsightsLogger.error("CQL Exception:"+query, e);
		}
		Rows<String, String> resultRows = result != null ? result.getResult().getRows() : null;
		return (resultRows != null && resultRows.size() > 0) ? resultRows.getRowByIndex(0).getColumns() : null ;
	}
	
	private ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {
		return new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());
	}
	
	private ColumnFamilyQuery<String, String> getColumnFamilyQuery(String columnFamilyName) {
		return getLogKeyspace().prepareQuery(accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	}
	
	private PreparedCqlQuery<String, String> setParameters(PreparedCqlQuery<String, String> cqlQuery, String... parameters) {
		
		for(int parameterCount = 0; parameterCount < parameters.length; parameterCount++) {
			if(StringUtils.isNotEmpty(parameters[parameterCount])) {
				cqlQuery.withStringValue(parameters[parameterCount]);	
			}
		}
		return cqlQuery;
	}
	
	//public static final String GET_SESSION_ACTIVITY_TYPE = "SELECT event_type FROM user_session_activity WHERE session_id = ? AND gooru_oid = ?";
	
	public ResultSet getSessionActivityType(String sessionId, String gooruOid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "user_session_activity")
				.where(QueryBuilder.eq("session_id", sessionId))
				.and(QueryBuilder.eq("gooru_oid", gooruOid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_COLLECTION_SESSIONS = "SELECT event_time,session_id,event_type FROM user_sessions WHERE user_uid = ? AND collection_uid = ? AND collection_type = ? AND class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ?";
	
	public ResultSet getUserCollectionSessions(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "user_sessions")
				.where(QueryBuilder.eq("user_uid", userUid))
				.and(QueryBuilder.eq("collection_uid", collectionUid))
				.and(QueryBuilder.eq("collection_type", collectionType))
				.and(QueryBuilder.eq("class_uid", classUid))
				.and(QueryBuilder.eq("course_uid", courseUid))
				.and(QueryBuilder.eq("unit_uid", unitUid))
				.and(QueryBuilder.eq("lesson_uid", lessonUid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_ASSESSMENT_SESSIONS = "SELECT event_time,session_id,event_type FROM user_sessions WHERE user_uid = ? AND collection_uid = ? AND collection_type = ? AND class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND event_type = ?";
	public ResultSet getUserAssessmentSessions(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid, String eventType) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "user_sessions")
				.where(QueryBuilder.eq("user_uid", userUid))
				.and(QueryBuilder.eq("collection_uid", collectionUid))
				.and(QueryBuilder.eq("collection_type", collectionType))
				.and(QueryBuilder.eq("class_uid", classUid))
				.and(QueryBuilder.eq("course_uid", courseUid))
				.and(QueryBuilder.eq("unit_uid", unitUid))
				.and(QueryBuilder.eq("lesson_uid", lessonUid))
				.and(QueryBuilder.eq("event_type", eventType));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_SESSION_ACTIVITY = "SELECT * FROM user_session_activity WHERE session_id = ?";
	
	public ResultSet getUserSessionActivity(String sessionId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "user_session_activity")
				.where(QueryBuilder.eq("session_id", sessionId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_SESSION_CONTENT_ACTIVITY = "SELECT * FROM user_session_activity WHERE session_id = ? AND gooru_oid = ? ";
	
	public ResultSet getUserSessionContentActivity(String sessionId, String gooruOid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "user_session_activity")
				.where(QueryBuilder.eq("session_id", sessionId))
				.and(QueryBuilder.eq("gooru_oid", gooruOid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ? AND user_uid = ? ";
	
	public ResultSet getUserCurrentLocationInClass(String classUid, String userUid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "student_location")
				.where(QueryBuilder.eq("class_uid", classUid))
				.and(QueryBuilder.eq("user_uid", userUid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_ALL_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ?";
	
	public ResultSet getAllUserCurrentLocationInClass(String classUid) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "student_location")
				.where(QueryBuilder.eq("class_uid", classUid))
				;
		return getCassSession().execute(select);
	}
	

	//public static final String GET_USER_CLASS_CONTENT_LATEST_SESSION = "SELECT session_id,user_uid FROM user_class_collection_last_sessions WHERE class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND collection_uid = ? AND user_uid = ?";
	
	public ResultSet getUserClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid, String userUid) {
		Statement select = QueryBuilder
				.select()
				.all()
				.from(getLogKeyspaceName(),
						"user_class_collection_last_sessions")
				.where(QueryBuilder.eq("class_uid", classUid))
				.and(QueryBuilder.eq("course_uid", courseUid))
				.and(QueryBuilder.eq("unit_uid", unitUid))
				.and(QueryBuilder.eq("lesson_uid", lessonUid))
				.and(QueryBuilder.eq("collection_uid", collectionUid))
				.and(QueryBuilder.eq("user_uid", userUid));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USERS_CLASS_CONTENT_LATEST_SESSION = "SELECT session_id,user_uid FROM user_class_collection_last_sessions WHERE class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND collection_uid = ?";
	public ResultSet getUsersClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid) {
		Statement select = QueryBuilder
				.select()
				.all()
				.from(getLogKeyspaceName(),
						"user_class_collection_last_sessions")
				.where(QueryBuilder.eq("class_uid", classUid))
				.and(QueryBuilder.eq("course_uid", courseUid))
				.and(QueryBuilder.eq("unit_uid", unitUid))
				.and(QueryBuilder.eq("lesson_uid", lessonUid))
				.and(QueryBuilder.eq("collection_uid", collectionUid));
		return getCassSession().execute(select);
	}

	//public static final String GET_USER_CLASS_ACTIVITY_DATACUBE = "SELECT * FROM class_activity_datacube WHERE row_key = ? AND user_uid = ? AND collection_type = ?";
	
	public ResultSet getUserClassActivityDatacube(String rowKey, String userUid, String collectionType) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "class_activity_datacube")
				.where(QueryBuilder.eq("row_key", rowKey))
				.and(QueryBuilder.eq("user_uid", userUid))
				.and(QueryBuilder.eq("collection_type", collectionType));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_CLASS_ACTIVITY_DATACUBE = "SELECT * FROM class_activity_datacube WHERE row_key = ? AND collection_type = ?";
	
	public ResultSet getClassActivityDatacube(String sessionId, String collectionType) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "class_activity_datacube")
				.where(QueryBuilder.eq("row_key", sessionId))
				.and(QueryBuilder.eq("collection_type", collectionType));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_USER_PEER_DETAIL = "SELECT * FROM class_activity_peer_detail WHERE row_key = ?";
	
	public ResultSet getUserPeerDetail(String rowKey) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "class_activity_peer_detail")
				.where(QueryBuilder.eq("row_key", rowKey))
				;
		return getCassSession().execute(select);
	}
	
	//Taxonomy Query's
	
	//public static final String GET_SUBJECT_ACTIVITY = " SELECT course_id,domain_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =?";
	public ResultSet getSubjectActivity(String rowKey, String subjectId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "content_taxonomy_activity")
				.where(QueryBuilder.eq("user_uid", rowKey))
				.and(QueryBuilder.eq("subject_id", subjectId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_COURSE_ACTIVITY = " SELECT domain_id,standards_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ?";
	public ResultSet getCourseActivity(String rowKey, String subjectId,
			String courseId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "content_taxonomy_activity")
				.where(QueryBuilder.eq("user_uid", rowKey))
				.and(QueryBuilder.eq("subject_id", subjectId))
				.and(QueryBuilder.eq("course_id", courseId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_DOMAIN_ACTIVITY = " SELECT standards_id,learning_targets_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ? AND domain_id = ?";
	public ResultSet getDomainActivity(String rowKey, String subjectId,
			String courseId, String domainId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "content_taxonomy_activity")
				.where(QueryBuilder.eq("user_uid", rowKey))
				.and(QueryBuilder.eq("subject_id", subjectId))
				.and(QueryBuilder.eq("course_id", courseId))
				.and(QueryBuilder.eq("domain_id", courseId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_STANDARDS_ACTIVITY = " SELECT learning_targets_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ? AND domain_id = ? AND standards_id = ?";
	public ResultSet getStandardsActivity(String rowKey, String subjectId,
			String courseId, String domainId, String standardsId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "content_taxonomy_activity")
				.where(QueryBuilder.eq("user_uid", rowKey))
				.and(QueryBuilder.eq("subject_id", subjectId))
				.and(QueryBuilder.eq("course_id", courseId))
				.and(QueryBuilder.eq("domain_id", domainId))
				.and(QueryBuilder.eq("standards_id", standardsId));
		return getCassSession().execute(select);
	}
	
	//public static final String GET_STUDENT_QUESTION_GRADE = "SELECT question_id,score FROM student_question_grade WHERE teacher_uid = ? AND user_uid = ? AND session_id = ?";
	public ResultSet getStandardsActivity(String teacherUid, String userUid,
			String sessionId) {
		Statement select = QueryBuilder.select().all()
				.from(getLogKeyspaceName(), "student_question_grade")
				.where(QueryBuilder.eq("teacher_uid", teacherUid))
				.and(QueryBuilder.eq("user_uid", userUid))
				.and(QueryBuilder.eq("session_id", sessionId));
		return getCassSession().execute(select);
	}
}
