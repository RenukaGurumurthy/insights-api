package org.gooru.insights.api.daos;

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.ColumnFamilySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Builder;
import com.datastax.driver.core.querybuilder.Select.Where;
@Repository
public class CqlCassandraDaoImpl extends CassandraConnectionProvider implements CqlCassandraDao {

	private final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

	private static final Logger LOG = LoggerFactory.getLogger(CqlCassandraDaoImpl.class);

	//public static final String GET_SESSION_ACTIVITY_TYPE = "SELECT event_type FROM user_session_activity WHERE session_id = ? AND gooru_oid = ?";
	@Override
	public ResultSet getSessionActivityType(String sessionId, String gooruOid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._SESSION_ID, sessionId))
					.and(QueryBuilder.eq(ApiConstants._GOORU_OID, gooruOid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			while (!resultSetFuture.isDone()) {
				LOG.debug("Waiting for request to complete...");
			}
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_USER_COLLECTION_SESSIONS = "SELECT event_time,session_id,event_type FROM user_sessions WHERE user_uid = ? AND collection_uid = ? AND collection_type = ? AND class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ?";
	@Override
	public ResultSet getUserCollectionSessions(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSIONS.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
					.and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, collectionUid))
					.and(QueryBuilder.eq(ApiConstants._COLLECTION_TYPE, collectionType))
					.and(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
					.and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseUid))
					.and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitUid))
					.and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonUid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_USER_ASSESSMENT_SESSIONS = "SELECT event_time,session_id,event_type FROM user_sessions WHERE user_uid = ? AND collection_uid = ? AND collection_type = ? AND class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND event_type = ?";
	@Override
	public ResultSet getUserAssessmentSessions(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid, String eventType) {
		Statement select;
		ResultSet result = null;
		try {
			if (eventType != null) {
				select = getUserSessionStatmentForAssessment(userUid, collectionUid, collectionType, classUid,
						courseUid, unitUid, lessonUid, eventType);
			} else {
				select = getUserSessionStatmentForCollection(userUid, collectionUid, collectionType, classUid,
						courseUid, unitUid, lessonUid, eventType);
			}
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_USER_SESSION_ACTIVITY = "SELECT * FROM user_session_activity WHERE session_id = ?";
	@Override
	public ResultSet getUserSessionActivity(String sessionId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._SESSION_ID, sessionId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_USER_SESSION_CONTENT_ACTIVITY = "SELECT * FROM user_session_activity WHERE session_id = ? AND gooru_oid = ? ";
	@Override
	public ResultSet getUserSessionContentActivity(String sessionId, String gooruOid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._SESSION_ID, sessionId))
					.and(QueryBuilder.eq(ApiConstants._GOORU_OID, gooruOid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ? AND user_uid = ? ";
	@Override
	public ResultSet getUserCurrentLocationInClass(String classUid, String userUid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.STUDENT_LOCATION.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
					.and(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_ALL_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ?";
	@Override
	public ResultSet getAllUserCurrentLocationInClass(String classUid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.STUDENT_LOCATION.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}


	//public static final String GET_USER_CLASS_CONTENT_LATEST_SESSION = "SELECT session_id,user_uid FROM user_class_collection_last_sessions WHERE class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND collection_uid = ? AND user_uid = ?";
	@Override
	public ResultSet getUserClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid, String userUid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.USER_CLASS_COLLECTION_LAST_SESSIONS.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
					.and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseUid))
					.and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitUid))
					.and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonUid))
					.and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, collectionUid))
					.and(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_USERS_CLASS_CONTENT_LATEST_SESSION = "SELECT session_id,user_uid FROM user_class_collection_last_sessions WHERE class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND collection_uid = ?";
	@Override
	public ResultSet getUsersClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.USER_CLASS_COLLECTION_LAST_SESSIONS.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
					.and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseUid))
					.and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitUid))
					.and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonUid))
					.and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, collectionUid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_USER_CLASS_ACTIVITY_DATACUBE = "SELECT * FROM class_activity_datacube WHERE row_key = ? AND user_uid = ? AND collection_type = ?";
	@Override
	public ResultSet getUserClassActivityDatacube(String rowKey, String userUid, String collectionType) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._ROW_KEY, rowKey))
					.and(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
					.and(QueryBuilder.eq(ApiConstants._COLLECTION_TYPE, collectionType))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_CLASS_ACTIVITY_DATACUBE = "SELECT * FROM class_activity_datacube WHERE row_key = ? AND collection_type = ?";
	@Override
	public ResultSet getClassActivityDatacube(String sessionId, String collectionType) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._ROW_KEY, sessionId))
					.and(QueryBuilder.eq(ApiConstants._COLLECTION_TYPE, collectionType))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_USER_PEER_DETAIL = "SELECT * FROM class_activity_peer_detail WHERE row_key = ?";
	@Override
	public ResultSet getUserPeerDetail(String rowKey) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CLASS_ACTIVITY_PEER_DETAIL.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._ROW_KEY, rowKey))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//Taxonomy Query's

	//public static final String GET_SUBJECT_ACTIVITY = " SELECT course_id,domain_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =?";
	@Override
	public ResultSet getSubjectActivity(String rowKey, String subjectId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._USER_UID, rowKey))
					.and(QueryBuilder.eq(ApiConstants._SUBJECT_ID, subjectId));
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_COURSE_ACTIVITY = " SELECT domain_id,standards_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ?";
	@Override
	public ResultSet getCourseActivity(String rowKey, String subjectId,
			String courseId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._USER_UID, rowKey))
					.and(QueryBuilder.eq(ApiConstants._SUBJECT_ID, subjectId))
					.and(QueryBuilder.eq(ApiConstants._COURSE_ID, courseId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture future = getCassSession().executeAsync(select);
			while (!future.isDone()) {
				LOG.debug("Waiting for request to complete...");
			}
			result = future.get();
		} catch (Exception e) {
			LOG.error("Exception:" + e);
		}

		return result;
	}

	//public static final String GET_DOMAIN_ACTIVITY = " SELECT standards_id,learning_targets_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ? AND domain_id = ?";
	@Override
	public ResultSet getDomainActivity(String rowKey, String subjectId,
			String courseId, String domainId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._USER_UID, rowKey))
					.and(QueryBuilder.eq(ApiConstants._SUBJECT_ID, subjectId))
					.and(QueryBuilder.eq(ApiConstants._COURSE_ID, courseId))
					.and(QueryBuilder.eq(ApiConstants._DOMAIN_ID, domainId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_STANDARDS_ACTIVITY = " SELECT learning_targets_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ? AND domain_id = ? AND standards_id = ?";
	@Override
	public ResultSet getStandardsActivity(String rowKey, String subjectId,
			String courseId, String domainId, String standardsId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._USER_UID, rowKey))
					.and(QueryBuilder.eq(ApiConstants._SUBJECT_ID, subjectId))
					.and(QueryBuilder.eq(ApiConstants._COURSE_ID, courseId))
					.and(QueryBuilder.eq(ApiConstants._DOMAIN_ID, domainId))
					.and(QueryBuilder.eq(ApiConstants._STANDARDS_ID, standardsId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_STUDENT_QUESTION_GRADE = "SELECT question_id,score FROM student_question_grade WHERE teacher_uid = ? AND user_uid = ? AND session_id = ?";
	@Override
	public ResultSet getStudentQuestionGrade(String teacherUid, String userUid,
			String sessionId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.STUDENT_QUESTION_GRADE.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._TEACHER_UID, teacherUid))
					.and(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
					.and(QueryBuilder.eq(ApiConstants._SESSION_ID, sessionId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	//public static final String GET_CLASS_COLLECTION_COUNT = "SELECT assessment_count,collection_count FROM class_collection_count WHERE class_uid=? AND collection_uid=?";
	@Override
	public ResultSet getClassCollectionCount(String classUid,
			String collectionUid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CLASS_CONTENT_COUNT.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
					.and(QueryBuilder.eq(ApiConstants._CONTENT_UID, collectionUid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	@Override
	public ResultSet getAuthorizedUsers(String gooruOid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CONTENT_AUTHORIZED_USERS_COUNT.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._GOORU_OID, gooruOid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	@Override
	public ResultSet getSesstionIdsByUserId(String userUid) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().column(ApiConstants._SESSION_ID)
					.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSIONS.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
		public ResultSet getTaxonomyItemCount(Set<String> ids) {
			ResultSet result = null;
			try {
				Statement select = QueryBuilder.select().all()
						.from(getLogKeyspaceName(), ColumnFamilySet.TAXONOMY_PARENT_NODE.getColumnFamily())
						.where(QueryBuilder.in(ApiConstants._ROW_KEY, ids.toArray()))
						.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
				ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
				result = resultSetFuture.get();
			} catch (Exception e) {
				LOG.error("Exception:", e);
			}
			return result;
		}

		private Statement getUserSessionStatmentForAssessment(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid, String eventType){
			return QueryBuilder.select().all()
                    .from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSIONS.getColumnFamily())
                    .where(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
                    .and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, collectionUid))
                    .and(QueryBuilder.eq(ApiConstants._COLLECTION_TYPE, collectionType))
                    .and(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
                    .and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseUid))
                    .and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitUid))
                    .and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonUid))
                    .and(QueryBuilder.eq(ApiConstants._EVENT_TYPE, eventType))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	}

	private Statement getUserSessionStatmentForCollection(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid, String eventType){
		return QueryBuilder.select().all()
				.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSIONS.getColumnFamily())
				.where(QueryBuilder.eq(ApiConstants._USER_UID, userUid))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, collectionUid))
				.and(QueryBuilder.eq(ApiConstants._COLLECTION_TYPE, collectionType))
				.and(QueryBuilder.eq(ApiConstants._CLASS_UID, classUid))
				.and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseUid))
				.and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitUid))
				.and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonUid))
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	}
	@Override
	public ResultSet getStatMetrics(String gooruOids) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.STATISTICAL_DATA.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants._CLUSTERING_KEY, gooruOids))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ResultSet getStudentsClassActivity(String classId, String courseId, String unitId, String lessonId,
			String collectionId) {
		ResultSet result = null;
		try {
			Builder builder = QueryBuilder.select().all();

			Select select = builder.from(getLogKeyspaceName(),
					ColumnFamilySet.STUDENTS_CLASS_ACTIVITY.getColumnFamily());

			Where where = select.where(QueryBuilder.eq(ApiConstants._CLASS_UID, classId));
			if (courseId != null) {
				where.and(QueryBuilder.eq(ApiConstants._COURSE_UID, courseId));
			}
			if (unitId != null) {
				where.and(QueryBuilder.eq(ApiConstants._UNIT_UID, unitId));
			}
			if (lessonId != null) {
				where.and(QueryBuilder.eq(ApiConstants._LESSON_UID, lessonId));
			}
			if (collectionId != null) {
				where.and(QueryBuilder.eq(ApiConstants._COLLECTION_UID, lessonId));
			}
			where.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	@Override
	public ResultSet getTaxonomyParents(String taxonomyIds) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.TAXONOMY_PARENT_NODE.getColumnFamily())
					.where(QueryBuilder.in(ApiConstants._ROW_KEY, (Object[]) taxonomyIds.split(",")))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	@Override
	public ResultSet getSessionResourceTaxonomyActivity(String sessionId, String gooruOid) {

		ResultSet result = null;
		try {
			Select select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.USER_SESSION_TAXONOMY_ACTIVITY.getColumnFamily());
			Where where = select.where(QueryBuilder.eq(ApiConstants._SESSION_ID, sessionId));

			if(StringUtils.isNotBlank(gooruOid)) {
				where.and(QueryBuilder.eq(ApiConstants._GOORU_OID, gooruOid));
			}
			where.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ResultSet getEvent(String eventId) {
		ResultSet result = null;
		try {
			Select select = QueryBuilder.select().all().from(getLogKeyspaceName(),
					ColumnFamilySet.EVENTS.getColumnFamily());
			Where where = select.where(QueryBuilder.eq(ApiConstants._EVENT_ID, eventId));
			where.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	@Override
	public ResultSet getArchievedClassMembers(String classId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.USER_GROUP_ASSOCIATION.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants.KEY, classId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	@Override
	public ResultSet getArchievedClassData(String rowKey) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.CLASS_ACTIVITY.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants.KEY, rowKey))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	@Override
	public ResultSet getArchievedContentTitle(String contentId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.DIM_RESOURCE.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants.KEY, "GLP~"+contentId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	@Override
	public ResultSet getArchievedUserDetails(String userId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.DIM_USER.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants.KEY, userId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ResultSet getArchievedCollectionItem(String contentId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.COLLECTION_ITEM_ASSOC.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants.KEY, contentId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ResultSet getArchievedCollectionRecentSessionId(String rowKey) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.SESSION.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants.KEY, rowKey))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ResultSet getArchievedSessionData(String sessionId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(getLogKeyspaceName(), ColumnFamilySet.SESSION_ACTIVITY.getColumnFamily())
					.where(QueryBuilder.eq(ApiConstants.KEY, sessionId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = getCassSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ProtocolVersion getClusterProtocolVersion(){
		return getProtocolVersion();
	}
}
