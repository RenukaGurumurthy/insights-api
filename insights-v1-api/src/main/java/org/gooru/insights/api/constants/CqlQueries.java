package org.gooru.insights.api.constants;

public final class CqlQueries {

	// Session Query's
	public static final String GET_SESSION_ACTIVITY_TYPE = "SELECT event_type FROM user_session_activity WHERE session_id = ? AND gooru_oid = ?";

	public static final String GET_USER_COLLECTION_SESSIONS = "SELECT event_time,session_id,event_type FROM user_sessions WHERE user_uid = ? AND collection_uid = ? AND collection_type = ? AND class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ?";

	public static final String GET_USER_ASSESSMENT_SESSIONS = "SELECT event_time,session_id,event_type FROM user_sessions WHERE user_uid = ? AND collection_uid = ? AND collection_type = ? AND class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND event_type = ?";

	public static final String GET_USER_SESSION_ACTIVITY = "SELECT * FROM user_session_activity WHERE session_id = ?";

	public static final String GET_USER_SESSION_CONTENT_ACTIVITY = "SELECT * FROM user_session_activity WHERE session_id = ? AND gooru_oid = ? ";

	// Location Query's
	public static final String GET_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ? AND user_uid = ? ";

	public static final String GET_ALL_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ?";

	// Class Query's

	public static final String GET_USER_CLASS_CONTENT_LATEST_SESSION = "SELECT session_id,user_uid FROM user_class_collection_last_sessions WHERE class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND collection_uid = ? AND user_uid = ?";

	public static final String GET_USERS_CLASS_CONTENT_LATEST_SESSION = "SELECT session_id,user_uid FROM user_class_collection_last_sessions WHERE class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid = ? AND collection_uid = ?";

	public static final String GET_USER_CLASS_ACTIVITY_DATACUBE = "SELECT * FROM class_activity_datacube WHERE row_key = ? AND user_uid = ? AND collection_type = ?";

	public static final String GET_CLASS_ACTIVITY_DATACUBE = "SELECT * FROM class_activity_datacube WHERE row_key = ? AND collection_type = ?";

	public static final String GET_USER_PEER_DETAIL = "SELECT * FROM class_activity_peer_detail WHERE row_key = ?";

	//Taxonomy Query's
	public static final String GET_SUBJECT_ACTIVITY = " SELECT course_id,domain_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =?";

	public static final String GET_COURSE_ACTIVITY = " SELECT domain_id,standards_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ?";

	public static final String GET_DOMAIN_ACTIVITY = " SELECT standards_id,learning_targets_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ? AND domain_id = ?";

	public static final String GET_STANDARDS_ACTIVITY = " SELECT learning_targets_id,resource_type,views,timespent,score FROM content_taxonomy_activity  WHERE user_uid = ? AND subject_id =? AND course_id = ? AND domain_id = ? AND standards_id = ?";

	public static final String GET_STUDENT_QUESTION_GRADE = "SELECT question_id,score FROM student_question_grade WHERE teacher_uid = ? AND user_uid = ? AND session_id = ?";


	private CqlQueries() {
		throw new AssertionError();
	}
}
