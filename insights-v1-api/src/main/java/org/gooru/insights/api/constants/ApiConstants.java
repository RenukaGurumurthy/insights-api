package org.gooru.insights.api.constants;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public final class ApiConstants {

	/**
	 * String constants
	 */

	public static final String DATA = "data";

	public static final String STRING_EMPTY= "";

	public static final String COMMA = ",";

	public static final String TILDA = "~";

	public static final String AS_TILDA = "AS"+TILDA;

	public static final String RS = "RS";

	public static final String QUESTION_MARK = "?";

	public static final String FORWARD_SLASH = "/";

	public static final String HYPHEN = "-";

	public static final String PIPE = "|";

	public static final String DOUBLE_QUOTES = "\"";

	public static final String REPLACER = "{0}";

	public static final String GOORU_SESSION_TOKEN = "Gooru-Session-Token";

	public static final String SESSION_TOKEN = "sessionToken";

	public static final String TOMCAT_INIT = "tomcat-init";

	public static final String GOORU_REST_ENDPOINT = "gooru.api.rest.endpoint";

	public static final String ENTITY_ROLE_OPERATIONS = "entity_role_opertaions";

	public static final String ROLE_GOORU_ADMIN = "ROLE_GOORU_ADMIN";

	public static final String CONTENT_ADMIN = "Content_Admin";

	public static final String ORGANIZATION_ADMIN = "Organization_Admin";

	public static final String USER_ROLE_SETSTRING = "userRoleSetString";

	public static final String CONSTANT_VALUE = "constant_value";

	public static final String USER_TOKEN = "userToken";

	public static final String USER = "user";

	public static final String REPORT = "Report";

	public static final String VIEW = "View";

	public static final String VALUE = "value";

	public static final String COLUMN1 = "column1";

	public static final  String COLUMNS_TO_EXPORT = "score_in_percentage|time_spent|views";

	public static final  String RESOURCE_COLUMNS_TO_EXPORT = ".*question_status.*|.*score_in_percentage.*|.*time_spent.*|.*views.*";

	public static final  String STRING_COLUMNS = ".*collection_type.*";

	public static final Pattern BIGINT_COLUMNS_PATTERN = Pattern.compile(".*score_in_percentage.*|.*time_spent.*|.*views.*");


	/**
	 * Mail Constants
	 */
	public static final String DEFAULT_MAIL = "insights@goorulearning.org";

	public static final String DEFAULT_MAIL_MESSAGE = "File download link will be sent to your email Account";

	public static final String MAIL_TEXT = "Hi,This report will take some more time to get process,we will send you this report to insights@goorulearning.org, Thanks";

	/**
	 * Exclude fields
	 */

	public static final String EXCLUDE_CLASSES = "*.class";

	/**
	 * HTTP constants
	 */
	public static final String STATUS_CODE = "statusCode";

	public static final String RESOURCE_GOORUOID = "resourceGooruOId";

	public static final String SESSION = "session";

	public static final String USER_UID = "userUId";

	public static final String USERUID = "userUid";

	public static final String CLASSID_USERUID = "classId or userUId";

	public static final String CLASSPAGE_GOORU_OID = "classpage_gooru_oid";

	public static final String COLLECTION_GOORU_OID = "collection_gooru_oid";

	public static final String IS_GROUP_OWWNER = "is_group_owner";

	public static final String DELETED = "deleted";

	public static final String ASSESSMENT_TYPES = "assessment/url|assessment";

	public static final String USERCOUNT = "userCount";

	public static final String GOORUOID = "gooruOId";

	public static final String _GOORUOID = "gooruOid";

	public static final String GOORU_UID = "gooru_uid";

	public static final String GOORUUID = "gooruUid";

	public static final String ORDER = "order";

	public static final String USER_NAME = "userName";

	public static final String LAST_ACCESSED = "lastAccessed";

	public static final String LAST_MODIFIED = "lastModified";

	public static final String NFS_BUCKET = "insights.nfs.bucket.path";

	public static final String FOLDER = "folder";

	public static final String URL = "url";

	public static final String THUMBNAIL = "thumbnail";

	public static final String ASC = "ASC";

	public static final String DESC = "DESC";

	public static final String FILTERS = "filters";

	public static final String GOORU_OID = "gooru_oid";

	public static final String QUESTION_GOORU_OID = "question_gooru_oid";

	public static final String _QUESTIONGOORUOID = "questionGooruOid";

	public static final String COLLECTIONGOORUOID = "collection_gooru_oid";

	public static final String TOTAL_INCORRECT_COUNT = "totalInCorrectCount";

	public static final String TOTAL_CORRECT_COUNT = "totalCorrectCount";

	public static final String RESOURCE_GOORU_OID = "resourceGooruOid";

	public static final String RESOURCEGOORUOID = "resource_gooru_oid";

	public static final String HTTP = "http";

	public static final String SEQUENCE = "sequence";

	public static final String HTTPS = "https";

	public static final String _HTTP = "http://";

	public static final String _HTTPS = "https://";

	public static final String _IS_CORRECT = "is_correct";

	public static final String IS_CORRECT = "isCorrect";

	public static final String ANSWER_TEXT = "answer_text";

	public static final String _ANSWER_OBJECT = "answer_object";

	public static final String ANSWER_OBJECT = "answerObject";

	public static final String _ANSWER_ID = "answer_id";
	public static final String ANSWER_ID = "answerId";
	public static final String _QUESTION_ID = "question_id";
	public static final String QUESTION_ID = "questionId";
	public static final String _TYPE_NAME = "type_name";

	public static final String _AVG_TIME_SPENT = "avg_time_spent";

	public static final String AVG_TIME_SPENT = "avgTimeSpent";

	public static final String _ANSWERTEXT = "answerText";

	public static final String ATTEMPTS = "attempts";

	public static final String OPTIONS = "options";

	public static final String COLLECTION_ITEM_ID = "collectionItemId";

	public static final String COLLECTIONITEMID = "collection_item_id";

	public static final String FEEDBACKPROVIDER = "feedbackProviderUId";

	public static final String _FEEDBACK_PROVIDER = "feed_back_provider";

	public static final String FEEDBACK_TEACHER_NAME = "feedbackTeacherName";

	public static final String USERNAME = "username";

	public static final String CATEGORY = "category";

	public static final String META_DATA = "metaData";

	public static final String RESPONSE = "response";

	public static final String QUESTION = "question";

	public static final String QUESTIONS = "questions";

	public static final String QUESTION_TYPE = "questionType";

	public static final String QUESTION_MATCH = "question.questionType|question.type";

	public static final String _QUESTION_TYPE = "question_type";

	public static final String _DISPLAY_CODE = "display_code";

	public static final String DISPLAY_CODE = "displayCode";

	public static final String FIRST_NAME = "firstName";

	public static final String LAST_NAME = "lastName";

	public static final String EMAIL_ID = "emailId";

	public static final String GOORU_U_ID = "gooruUId";

	public static final String PARTY_UId = "partyUid";

	public static final String _USER_ID = "user_id";

	public static final String ID = "id";

	public static final String KEY = "key";
	public static final String USER_PROFILE_URL_PATH = "insights.profile.url.path";
	public static final String PROFILE_URL = "profileUrl";

	public static final String LESSON = "lesson";
	public static final String SUBJECT = "subject";
	public static final String COURSE = "course";
	public static final String DOMAIN = "domain";
	public static final String STANDARDS = "standards";
	public static final String LEARNING_TARGETS = "learningTargets";
	public static final String UNIT = "unit";
	public static final String CLASS = "class";
	public static final String INPROGRESS = "in-progress";
	public static final String COMPLETED = "completed";
	public static final String BOTH = "both";
	public static final String START = "start";
	public static final String STOP = "stop";
	public static final String EVENT_TIME = "eventTime";
	public static final String SESSIONID = "sessionId";

	public static final String _SESSION_ID = "session_id";

	public static final String ITEM = "item";
	public static final String COLLECTION_MATCH = "collection|scollection";
	public static final String COLLECTION = "collection";
	public static final String ASSESSMENT = "assessment";
	public static final String CORRECT = "correct";
	public static final String IN_CORRECT = "in_correct";

	public static final String ASSESSMENT_QUESTION_TYPES = "assessment-question|question";

	public static final String ASSESSMENT_SLASH_URL = "assessment/url";
	public static final String LESSON_COUNT = "lessonCount";
	public static final String UNIT_COUNT = "unitCount";
	public static final String COURSE_COUNT = "courseCount";
	public static final String COLLECTION_COUNT = "collectionCount";
	public static final String ASSESSMENT_COUNT = "assessmentCount";
	public static final String RESOURCE_COUNT = "resourceCount";
	public static final String QUESTION_COUNT = "questionCount";
	public static final String OE_COUNT = "oeCount";
	public static final String _RESOURCE_COUNT = "resource_count";
	public static final String _QUESTION_COUNT = "question_count";
	public static final String _OE_COUNT = "oe_count";

	public static final String SCORABLE_QUESTION_COUNT = "scorableQuestionCount";
	public static final String SCORABLE_COUNT_ON_EVENT = "selectedSessionScorableQuestionCount";
	public static final String EXTERNAL_ASSESSMENT_COUNT = "externalAssessmentCount";
	public static final String ITEM_COUNT = "itemCount";
	public static final String COLLECTIONS_VIEWED = "collectionsViewed";
	public static final String ASSESSMENTS_ATTEMPTED = "assessmentsAttempted";
	public static final String SCORE_MET = "ScoreMet";
	public static final String SCORE_NOT_MET = "ScoreNotMet";
	public static final String NOT_ATTEMPTED = "NotAttempted";
	public static final String NOT_SCORED = "NotScored";
	public static final String VIEWED = "Viewed";
	public static final String NOT_VIEWED = "NotViewed";
	public static final String SCORE_STATUS = "scoreStatus";
	public static final String MINIMUM_SCORE = "minimum_score";
	public static final String GOAL = "goal";
	public static final String _LAST_ACCESSED = "last_accessed";
	public static final String EVIDENCE = "evidence";
	public static final String USAGE_DATA = "usageData";
	public static final String TITLE = "title";
	public static final String TYPE = "type";
	public static final String RESOURCE_TYPE = "resourceType";
	public static final String RESOURCE_MATCH = "resourceType";
	public static final String RESOURCE_FORMAT = "resourceFormat";
	public static final String VIEWS = "views";
	public static final String TIMESPENT = "timeSpent";
	public static final String _TIME_SPENT = "time_spent";
	public static final String _ATTEMPT_STATUS = "attempt_status";
	public static final String TOTAL_STUDY_TIME = "totalStudyTime";
	public static final String TOTAL_SCORE = "totalScore";
	public static final String AVG_SCORE = "avgScore";
	public static final String _COLLECTION_TYPE = "collection_type";
	public static final String COLLECTION_TYPE = "collectionType";
	public static final String SCORE_IN_PERCENTAGE = "scoreInPercentage";
	public static final String _SCORE_IN_PERCENTAGE = "score_in_percentage";
	public static final String SCORE = "score";
	public static final String CLASS_USAGE_DATA = "classUsageData";
	public static final String COURSE_USAGE_DATA = "courseUsageData";
	public static final String UNIT_USAGE_DATA = "unitUsageData";
	public static final String LESSON_USAGE_DATA = "lessonUsageData";
	public static final String COLLECTION_USAGE_DATA = "collectionUsageData";
	public static final String ASSESSMENT_USAGE_DATA = "assessmentUsageData";
	public static final String OPEN_BRACE = "{";
	public static final String CLOSE_BRACE = "}";
	public static final String UTF8 = "UTF-8";
	public static final String TIME_SPENT = "timespent";

	public static final String HAS_FRAME_BREAKER = "hasFrameBreaker";
	public static final String QUESTION_DOT_TYPE = "question.type";
	public static final String QUESTION_DOT_QUESTION_TYPE = "question.questionType";
	public static final String REACTION = "reaction";
	public static final String CHOICE = "choice";
	public static final String SKIPPED = "skipped";
	public static final String TEXT = "text";
	public static final String RA = "RA";
	public static final String _REACTION_COUNT = "reaction_count";
	public static final String _QUESTION_STATUS = "question_status";
	public static final String STATUS = "status";
	public static final String TAU = "tau";
	public static final String TOTAL_ATTEMPT_USER_COUNT = "totalAttemptUserCount";
	public static final String OPTIONS_MATCH = "A|B|C|D|E|F";
	public static final String _AVG_REACTION = "avg_reaction";
	public static final String AVG_REACTION = "avgReaction";
	public static final String RESOURCE = "resource";
	public static final String CSV_EXT = ".csv";
	public static final String _ITEM_COUNT = "item_count";
	public static final String _CREATOR_UID = "creator_uid";
	public static final String COLLABORATORS = "collaborators";
	public static final String CLASS_GOORU_ID = "classGooruId";
	public static final String BEAN_INIT = "Bean-init";
	public static final String _ASSESSMENT_UNIQUE_VIEWS = "assessment_unique_views";
	public static final String _COLLECTION_UNIQUE_VIEWS = "collection_unique_views";
	public static final String _UNIQUE_VIEWS = "unique_views";
	public static final String _TOTAL_REACTION = "total_reaction";
	public static final String _REACTED_COUNT = "reacted_count";
	public static final String VISIBILITY = "visibility";

	public static final String _EVENT_TYPE = "event_type";
	public static final String _COLLECTION_UID = "collection_uid";
	public static final String _CONTENT_UID = "content_uid";
	public static final String _USER_UID = "user_uid";
	public static final String _CLASS_UID = "class_uid";
	public static final String _COURSE_UID = "course_uid";
	public static final String _UNIT_UID = "unit_uid";
	public static final String _LESSON_UID = "lesson_uid";
	public static final String _EVENT_TIME = "event_time";
	public static final String _EVENT_ID = "event_id";
	public static final String ACTIVE_PEER_COUNT = "activePeerCount";
	public static final String LEFT_PEER_COUNT = "leftPeerCount";
	public static final String _LEAF_GOORU_OID = "leaf_gooru_oid";
	public static final String _ACTIVE_PEER_COUNT = "active_peer_count";
	public static final String _LEFT_PEER_COUNT = "left_peer_count";
	public static final String _GOORU_OID = "gooru_oid";
	public static final String _RESOURCE_FORMAT = "resource_format";
	public static final String _RESOURCE_TYPE = "resource_type";
	public static final String RESOURCES = "resources";
	public static final String _ROW_KEY = "row_key";
	public static final String _LEAF_NODE = "leaf_node";
	public static final String _ID = "Id";
	public static final String CONTENT = "content";
	public static final String _LEVEL_TYPE = "level_type";
	public static final Pattern COLLECTION_OR_ASSESSMENT_PATTERN = Pattern.compile("collection|assessment|content");
	public static final String _SUBJECT_ID = "subject_id";
	public static final String _COURSE_ID = "course_id";
	public static final String _DOMAIN_ID = "domain_id";
	public static final String _SUB_DOMAIN_ID = "sub_domain_id";
	public static final String _STANDARDS_ID = "standards_id";
	public static final String _LEARNING_TARGETS_ID = "learning_targets_id";
	public static final String TOTAL_COUNT = "totalCount";
	public static final String _TOTAL_COUNT = "total_count";
	public static final String COMPLETED_COUNT = "completedCount";
	public static final String _COMPLETED_COUNT = "completed_count";
	public static final String ACTIVE_PEER_UIDS = "activePeerUids";
	public static final String LEFT_PEER_UIDS = "leftPeerUids";
	public static final String _ACTIVE_PEERS = "active_peers";
	public static final String _LEFT_PEERS = "left_peers";
	public static final String COURSE_IDS = "courseIds";
	public static final String NA = "NA";
	public static final String _TEACHER_UID = "teacher_uid";
	public static final String ANSWER_STATUS = "answer_status";
	public static final String RESOURCE_IDS = "resourceIds";
	public static final String _SESSION_TIME = "session_time";
	public static final String PEER_COUNT = "peerCount";
	public static final String ACTIVE = "Active";
	public static final String IN_ACTIVE = "Inactive";
	public static final String _PARENT_EVENT_ID = "parent_event_id";
	public static final String PARENT_EVENT_ID = "parentEventId";
	public static final String _COLLECTION_COUNT = "collection_count";
	public static final String _ASSESSMENT_COUNT = "assessment_count";
	public static final String _CLUSTERING_KEY = "clustering_key";
	public static final String _METRICS_NAME = "metrics_name";
	public static final String _METRICS_VALUE = "metrics_value";

	private ApiConstants() {
		throw new AssertionError();
	}

	public enum Numbers {
		FOUR("4"), FIVE("5");

		private final String number;

		public String getNumber() {
		return number;
		}

		Numbers(String number) {
		this.number = number;
		}
	}

	public enum ColumnFamilySet {
		RESOURCE("resource"), DIM_RESOURCE("dim_resource"), REAL_TIME_DASHBOARD(
				"real_time_aggregator"), CUSTOM_FIELDS("custom_fields_data"), LIVE_DASHBOARD(
				"live_dashboard"), COLLECTION("collection"), COLLECTION_ITEM(
				"collection_item"),COLLECTION_ITEM_ASSOC("collection_item_assoc"), CLASSPAGE("classpage"), ASSESSMENT_ANSWER(
				"assessment_answer"), MICRO_AGGREGATION("micro_aggregation"), FORMULA_DETAIL(
				"formula_detail"), EVENT_TIMELINE("event_timeline"), EVENT_DETAIL(
				"event_detail"), USERPROFILE("user_profile_settings"), USER_COLLECTION_ITEM_ASSOC(
				"user_collection_item_assoc"), CONFIG_SETTING(
				"job_config_settings"), USER("user"),DIM_USER("dim_user"), SESSION("sessions"), SESSION_ACTIVITY("session_activity")
				,CLASS_ACTIVITY("class_activity"), SESSION_ACTIVITY_COUNTER("session_activity_counter"),
				CLASS("class"),USER_GROUP_ASSOCIATION("user_group_association"),CONTENT_META("content_meta"),TABLE_DATATYPES("table_datatypes"),JOB_TRACKER("job_tracker"),CLASS_COLLECTION_SETTINGS("class_collection_settings")
				,STUDENT_LOCATION("student_location"), USER_SESSIONS("user_sessions"),
				USER_SESSION_ACTIVITY("user_session_activity"),CLASS_ACTIVITY_DATACUBE("class_activity_datacube"),
				CONTENT_TAXONOMY_ACTIVITY("content_taxonomy_activity"), CLASS_ACTIVITY_PEER_DETAIL("class_activity_peer_detail"),
				STUDENT_QUESTION_GRADE("student_question_grade"), USER_CLASS_COLLECTION_LAST_SESSIONS("user_class_collection_last_sessions"),CLASS_CONTENT_COUNT("class_content_count"),CONTENT_AUTHORIZED_USERS_COUNT("content_authorized_users"),STATISTICAL_DATA("statistical_data"),STUDENTS_CLASS_ACTIVITY("students_class_activity"),
				TAXONOMY_PARENT_NODE("taxonomy_parent_node"),USER_SESSION_TAXONOMY_ACTIVITY("user_session_taxonomy_activity"),EVENTS("events");

		private final String columnFamily;

		ColumnFamilySet(String value) {
			columnFamily = value;
		}

		public String getColumnFamily() {
			return columnFamily;
		}
	}

	public enum modelAttributes {
		VIEW_NAME("content"), CONTENT("content"), RETURN_NAME("content"), MESSAGE("message"), PAGINATE(
				"paginate"), TOTAL_ROWS("totalRows");
		private final String attribute;

		modelAttributes(String attribute) {
			this.attribute = attribute;
		}

		public String getAttribute() {
			return attribute;
		}
	}


	/**
	 * Message constants
	 */
	public static final String COURSE_PLAN_UNAVAILABLE = "Course Plan unavailable for CourseGooruOid : {} ";
	public static final String MESSAGE = "message";

	private static final Map<String, String> classHierarchyIdNameAsMap;

	static {
		classHierarchyIdNameAsMap = new HashMap<>();
		classHierarchyIdNameAsMap.put("class", "classId");
		classHierarchyIdNameAsMap.put("course", "courseId");
		classHierarchyIdNameAsMap.put("unit", "unitId");
		classHierarchyIdNameAsMap.put("lesson", "lessonId");
		classHierarchyIdNameAsMap.put("assessment", "assessmentId");
		classHierarchyIdNameAsMap.put("collection", "collectionId");
		classHierarchyIdNameAsMap.put("content", "gooruOId");
	}

	public static String getResponseNameByType(String type) {
		return StringUtils.defaultIfEmpty(classHierarchyIdNameAsMap.get(type), type);
	}

	public enum apiHeaders{
		ACCEPT("Accept"),JSON_HEADER("application/json"),XLS_HEADER("application/vnd.ms-excel"),XLS_RESPONSE("application/xls"),CSV_RESPONSE("application/csv");

		private final String header;

		apiHeaders(String header){
			this.header = header;
		}
		public String apiHeader(){
			return header;
		}
	}

}
