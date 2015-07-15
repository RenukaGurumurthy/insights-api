package org.gooru.insights.api.constants;

public class ApiConstants {

	/**
	 * String constants
	 */
	
	public static final String DATA = "data";
	
	public static final String STRING_EMPTY= "";
	 
	public static final String COMMA = ",";
	
	public static final String TILDA = "~";
	
	public static final String FORWARD_SLASH = "/";
	
	public static final String HYPHEN = "-";
	
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
	
	/**
	 * Mail Constants
	 */
	public static final String DEFAULT_MAIL = "insights@goorulearning.org";
	
	public static final String DEFAULT_MAIL_MESSAGE = "File download link will be sent to your email Account";

	public static final String MAIL_TEXT = "Hi,This report will take some more time to get process,we will send you this report to insights@goorulearning.org, Thanks";
	
	/**
	 * view constants
	 */
	public enum modelAttributes{
		VIEW_NAME("content"),CONTENT("content"),RETURN_NAME("content"),MESSAGE("message"),PAGINATE("paginate"),TOTAL_ROWS("totalRows");
		
		private String attribute;
		
		private modelAttributes(String attribute){
			this.attribute = attribute;
		}
		public String getAttribute(){
			return attribute;
		}
	}
	
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
	
	public static final String USERUID = "userUId";
	
	public static final String CLASSID_USERUID = "classId or userUId";
	
	public static final String CLASSPAGE_GOORU_OID = "classpage_gooru_oid";
	
	public static final String COLLECTION_GOORU_OID = "collection_gooru_oid";
	
	public static final String IS_GROUP_OWWNER = "is_group_owner";
	
	public static final String DELETED = "deleted";
	
	public static final String ASSESMENT_TYPE_MATCHER = "assessment/url|assessment";
	
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
	
	public static final String IS_CORRECT = "is_correct";
	
	public static final String _ISCORRECT = "isCorrect";
	
	public static final String ANSWER_TEXT = "answer_text";
	
	public static final String _ANSWER_OBJECT = "answer_object";
	
	public static final String ANSWER_OBJECT = "answerObject";
	
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
	
	public static final String QUESTION_TYPE = "questionType";
	
	public static final String _QUESTION_TYPE = "question_type";
	
	public static final String FIRST_NAME = "firstName";
	
	public static final String LAST_NAME = "lastName";
	
	public static final String EMAIL_ID = "emailId";
	
	public static final String GOORU_U_ID = "gooruUId";
	
	public static final String PARTY_UId = "partyUid";
	
	public static final String ID = "id";

	public static final String USER_PROFILE_URL_PATH = "insights.profile.url.path";
	public static final String PROFILE_URL = "profileUrl";

	public static final String LESSON = "lesson";
	public static final String COURSE = "course";
	public static final String UNIT = "unit";
	public static final String CLASS = "class";
	public static final String INPROGRESS = "in-progress";
	public static final String COMPLETED = "completed";
	public static final String START = "start";
	public static final String STOP = "stop";
	
	public static final String SESSIONID = "sessionId";
	
	public static final String _SESSION_ID = "session_id";
	
	public static final String ITEM = "item";
	public static final String COLLECTION_MATCH = "collection|scollection";
	public static final String COLLECTION = "collection";
	public static final String ASSESSMENT = "assessment";
	
	public static final String ASSESSMENT_MATCH = "assessment/url|assessment-question";
	
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

	public static final String EXTERNAL_ASSESSMENT_COUNT = "externalAssessmentCount";
	public static final String ITEM_COUNT = "itemCount";
	public static final String COLLECTIONS_VIEWED = "collectionsViewed";
	public static final String ASSESSMENTS_ATTEMPTED = "assessmentsAttempted";
	public static final String SCORE_MET = "ScoreMet";
	public static final String SCORE_NOT_MET = "ScoreNotMet";
	public static final String NOT_ATTEMPTED = "NotAttempted";
	public static final String NOT_SCORED = "NotScored";
	public static final String SCORE_STATUS = "scoreStatus";
	public static final String MINIMUM_SCORE = "minimum_score";
	public static final String GOAL = "goal";
	public static final String _LAST_ACCESSED = "last_accessed";
	public static final String EVIDENCE = "evidence";
	public static final String USAGE_DATA = "usageData";
	public static final String TITLE = "title";
	public static final String TYPE = "type";
	public static final String RESOURCE_TYPE = "resourceType";
	public static final String RESOURCE_FORMAT = "resourceFormat";
	public static final String VIEWS = "views";
	public static final String TIMESPENT = "timeSpent";
	public static final String _TIME_SPENT = "time_spent";
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
	
	public static final String HAS_FRAME_BREAKER = "hasFrameBreaker";
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
	
	public enum apiHeaders{
		ACCEPT("Accept"),JSON_HEADER("application/json"),XLS_HEADER("application/vnd.ms-excel"),XLS_RESPONSE("application/xls"),CSV_RESPONSE("application/csv");
		
		private String header;
		
		private apiHeaders(String header){
			this.header = header;
		}
		public String apiHeader(){
			return header;
		}
	}
	
	public enum SessionAttributes{
		CS("CS"),AS("AS"),FS("FS"),RS("RS");
		
		private String session;
		
		private SessionAttributes(String session){
			this.session = session;
		}
		public String getSession(){
			return session;
		}
	}
	public enum fileAttributes{
		XLS("xls"),CSV("csv"),JSON("json");
		
		private String attribute;
		
		private fileAttributes(String attribute){
			this.attribute = attribute;
		}
		public String attribute(){
			return attribute;
		}
	}

	public static enum Numbers {
		FOUR("4"), FIVE("5");

		private String number;

		public String getNumber() {
		return number;
		}

		private Numbers(String number) {
		this.number = number;
		}
	}
	
	public enum addQuestionType {
		OE("OE"),FIB("FIB"),MC("MC"),MA("MA"),TF("TF");
		
		private String questionType;
		
		private addQuestionType(String questionType){
			this.questionType = questionType;
		}
		public String getQuestionType(){
			return questionType;
		}
	}
	
	public enum options {
		A("A"),B("B"),C("C"),D("D"),E("E"),F("F");
		
		private String option;
		
		private options(String option){
			this.option = option;
		}
		public String option(){
			return option;
		}
	}
	
	public enum reactions {
		ONE("I need help"),TWO("I dont understand"),THREE("meh"),FOUR("I understand"),FIVE("I can explain"),TEN("-");
		
		private String reaction;
		
		private reactions(String reaction){
			this.reaction = reaction;
		}
		public String reaction(){
			return reaction;
		}
	}
}
