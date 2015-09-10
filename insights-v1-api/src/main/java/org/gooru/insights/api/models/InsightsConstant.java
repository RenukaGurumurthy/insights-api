package org.gooru.insights.api.models;

public interface InsightsConstant {

	/**
	 * cache constants
	 */
	String CACHE_PREFIX = "insights";

	String CACHE_PREFIX_ID = "key";

	String EMPTY= "";
	
	String COMMA = ",";

	String DATA_OBJECT = "data";

	String GOORU_PREFIX = "authenticate_";

	String SEPARATOR = "~";

	String GOORUUID = "gooruUId";
	
	String GOORUOID = "gooruOid";
	
	String SEQUENCE = "sequence";
	
	String SESSION_ID = "sessionId";
	
	String EVENT_TIME = "eventTime";
	
	String LAST_ACCESSED_RESOURCE = "lastAccessedResource";
	
	String _LAST_ACCESSED_RESOURCE = "last_accessed_resource";
	
	String TYPE = "type";
	
	String START = "start";
	
	String STOP = "stop";
	
	String INFO = "info";
	
	String NEW_QUERY = "new query:";
	
	String WILD_CARD = "*";
	
	String COLLECTION = "collection";
	
	String ASSESSMENT = "assessment";
	
	
	/**
	 * Serializer Excludes
	 */
	String EXCLUDE_CLASSES = "*.class";

	String EVENT_NAME = "event_name";

	String STATUS = "status";

	String CREATED = "created";

	public enum ColumnFamily {
		RESOURCE("resource"), DIM_RESOURCE("dim_resource"), REAL_TIME_DASHBOARD(
				"real_time_aggregator"), CUSTOM_FIELDS("custom_fields_data"), LIVE_DASHBOARD(
				"live_dashboard"), COLLECTION("collection"), COLLECTION_ITEM(
				"collection_item"),COLLECTION_ITEM_ASSOC("collection_item_assoc"), CLASSPAGE("classpage"), ASSESSMENT_ANSWER(
				"assessment_answer"), MICRO_AGGREGATION("micro_aggregation"), FORMULA_DETAIL(
				"formula_detail"), EVENT_TIMELINE("event_timeline"), EVENT_DETAIL(
				"event_detail"), USERPROFILE("user_profile_settings"), USER_COLLECTION_ITEM_ASSOC(
				"user_collection_item_assoc"), CONFIG_SETTING(
				"job_config_settings"), USER("user"), SESSION("sessions"), SESSION_ACTIVITY("session_activity")
				,CLASS_ACTIVITY("class_activity"), SESSION_ACTIVITY_COUNTER("session_activity_counter"), 
				CLASS("class"),USER_GROUP_ASSOCIATION("user_group_association"),CONTENT_META("content_meta"),TABLE_DATATYPES("table_datatypes"),JOB_TRACKER("job_tracker");

		private String columnFamily;

		private ColumnFamily(String value) {
			columnFamily = value;
		}

		public String getColumnFamily() {
			return columnFamily;
		}
	}


	public enum formulaDetail {
		ID("id"), EVENTS("events"), REQUESTVALUES("requestValues"), CREATEDON(
				"createdOn"), FORMULA("formula"), AGGREGATETYPE(
				"aggregate_type"), DEFAULT_AGGREGATETYPE("normal"), FORMULAS(
				"formulas"), NAME("name"), STATUS("status");

		private String name;

		private formulaDetail(String data) {
			name = data;
		}

		public String getName() {
			return name;
		}
	}
	
	public static enum DateFormats {

		DEFAULT("yyyy-MM-dd hh:kk:ss"), YEAR("yyyy"), QUARTER("yyyy-MM-dd"), MONTH(
				"yyyy-MM"), WEEK("yyyy-MM-dd"), DAY("yyyy-MM-dd"), HOUR(
				"yyyy-MM-dd hh"), MINUTE("yyyy-MM-dd hh:kk"), SECOND(
				"yyyy-MM-dd hh:kk:ss"), MILLISECOND("yyyy-MM-dd hh:kk:ss.SSS"), NANOSECOND(
				"yyyy-MM-dd hh:kk:ss.SSS");

		private String format;

		public String format() {
			return format;
		}

		private DateFormats(String format) {
			this.format = format;
		}
	}

}
