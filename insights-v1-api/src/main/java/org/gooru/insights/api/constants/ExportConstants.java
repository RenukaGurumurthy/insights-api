package org.gooru.insights.api.constants;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class ExportConstants {

	public static Map<String,String> csvHeaders;
	
	public static final String TIME_SPENT = "Timespent";
	
	public static final String VIEWS = "Views";
	
	public static final String ATTEMPTS = "attempts";
	
	public static final String COLLECTION_TS = "Collection Timespent";
	
	public static final String ASSESSMENT_TS = "Assessment Timespent";
	
	public static final String SCORE = "Score";
	
	public static final String STUDNETS_NAME = "Students Name";
	
	public static final String TITLE = "Title";
	
	public static final String GOORU_OID = "Gooru ID";
	
	public static final String USER_ID = "User ID";
	
	public static final String FIRST_NAME = "First Name";
	
	public static final String LAST_NAME = "Last Name";
	
	
	static {
		csvHeaders = new HashMap<String, String>();
		csvHeaders.put("score_in_percentage", SCORE);
		csvHeaders.put("time_spent", TIME_SPENT);
		csvHeaders.put("views", VIEWS);
	}
	public static String csvHeaders(String eventName) {
		return StringUtils.defaultIfEmpty(csvHeaders.get(eventName), eventName);
	}
}
