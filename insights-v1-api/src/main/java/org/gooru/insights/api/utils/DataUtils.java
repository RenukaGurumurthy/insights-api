package org.gooru.insights.api.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.options;

public class DataUtils {

	private static Map<String,String> stringColumns;

	private static Map<String,String> longColumns;
	
	private static Set<String> studentsCollectionUsageColumnSuffix;
	
	private static Collection<String> collectionSummaryResourceColumns;
	
	static{
		putStudentCollectionUsageCache();
		putCollectionSummaryUsageCache();
	}

	private static void putStudentCollectionUsageCache(){
		putStudentCollectionColumnSuffix();
	}
	
	private static void putCollectionSummaryUsageCache(){
		putcollectionSummaryResourceColumn();
	}
	
	private static void putcollectionSummaryResourceColumn(){
		collectionSummaryResourceColumns = new ArrayList<String>();
		collectionSummaryResourceColumns.add(ApiConstants.TITLE);
		collectionSummaryResourceColumns.add(ApiConstants.THUMBNAIL);
		collectionSummaryResourceColumns.add(ApiConstants.GOORUOID);
		collectionSummaryResourceColumns.add(ApiConstants.RESOURCE_FORMAT);
		collectionSummaryResourceColumns.add(ApiConstants.RESOURCE_TYPE);
		collectionSummaryResourceColumns.add(ApiConstants.CATEGORY);
		collectionSummaryResourceColumns.add(ApiConstants.HAS_FRAME_BREAKER);
		collectionSummaryResourceColumns.add(ApiConstants.QUESTION_DOT_TYPE);
		collectionSummaryResourceColumns.add(ApiConstants.QUESTION_DOT_QUESTION_TYPE);
	}
	
	private static void putStudentCollectionColumnSuffix(){
		studentsCollectionUsageColumnSuffix = new HashSet<String>();
		studentsCollectionUsageColumnSuffix.add(ApiConstants.VIEWS);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._TIME_SPENT);
		studentsCollectionUsageColumnSuffix.add(ApiConstants.SCORE);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._QUESTION_STATUS);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._ANSWER_OBJECT);
		studentsCollectionUsageColumnSuffix.add(ApiConstants.CHOICE);
		studentsCollectionUsageColumnSuffix.add(ApiConstants.ATTEMPTS);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._AVG_TIME_SPENT);
		studentsCollectionUsageColumnSuffix.add(ApiConstants._TIME_SPENT);
		studentsCollectionUsageColumnSuffix.add(ApiConstants.OPTIONS);
		studentsCollectionUsageColumnSuffix.add(options.A.name());
		studentsCollectionUsageColumnSuffix.add(options.B.name());
		studentsCollectionUsageColumnSuffix.add(options.C.name());
		studentsCollectionUsageColumnSuffix.add(options.D.name());
		studentsCollectionUsageColumnSuffix.add(options.E.name());
		studentsCollectionUsageColumnSuffix.add(options.F.name());
	}
	
	static{
		longColumns = new HashMap<String, String>();
		longColumns.put(ApiConstants.VIEWS, ApiConstants.VIEWS);
		longColumns.put(ApiConstants._SCORE_IN_PERCENTAGE, ApiConstants.SCORE_IN_PERCENTAGE);
		longColumns.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		longColumns.put(ApiConstants.REACTION, ApiConstants.REACTION);
		longColumns.put(ApiConstants.SCORE, ApiConstants.SCORE);
		longColumns.put(ApiConstants.SKIPPED, ApiConstants.SKIPPED);
		longColumns.put("attempts", "attempts");
		longColumns.put("correct", "totalCorrectCount");
		longColumns.put("in_correct", "totalInCorrectCount");
		longColumns.put("A", "options");
		longColumns.put("B", "options");
		longColumns.put("C", "options");
		longColumns.put("D", "options");
		longColumns.put("E", "options");
		longColumns.put("F", "options");
	}
	
	static{
		stringColumns = new HashMap<String, String>();
		stringColumns.put(ApiConstants.CHOICE, ApiConstants.TEXT);
		stringColumns.put(ApiConstants._FEEDBACK_PROVIDER, ApiConstants.FEEDBACKPROVIDER);
		stringColumns.put(ApiConstants._QUESTION_STATUS, ApiConstants.STATUS);
		stringColumns.put("answer_object", "answerObject");
	}

	private static Map<String,String> sessionActivityMetrics;

	static {
		sessionActivityMetrics = new HashMap<String, String>();
		sessionActivityMetrics.put(ApiConstants._COLLECTION_TYPE, ApiConstants.COLLECTION_TYPE);
		sessionActivityMetrics.put(ApiConstants.VIEWS, ApiConstants.VIEWS);
		sessionActivityMetrics.put(ApiConstants._SCORE_IN_PERCENTAGE, ApiConstants.SCORE_IN_PERCENTAGE);
		sessionActivityMetrics.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		sessionActivityMetrics.put(ApiConstants.REACTION, ApiConstants.REACTION);
		sessionActivityMetrics.put(ApiConstants.CHOICE, ApiConstants.TEXT);
		sessionActivityMetrics.put(ApiConstants.TYPE, ApiConstants.QUESTION_TYPE);
		sessionActivityMetrics.put(ApiConstants._FEEDBACK_PROVIDER, ApiConstants.FEEDBACKPROVIDER);
		sessionActivityMetrics.put(ApiConstants._QUESTION_STATUS, ApiConstants.STATUS);
		sessionActivityMetrics.put(ApiConstants.SCORE, ApiConstants.SCORE);
		sessionActivityMetrics.put(ApiConstants.TAU, ApiConstants.TOTAL_ATTEMPT_USER_COUNT);
		sessionActivityMetrics.put(ApiConstants.SKIPPED, ApiConstants.SKIPPED);
		sessionActivityMetrics.put(ApiConstants.ATTEMPTS, ApiConstants.ATTEMPTS);
		sessionActivityMetrics.put(ApiConstants.CORRECT, ApiConstants.TOTAL_CORRECT_COUNT);
		sessionActivityMetrics.put(ApiConstants.IN_CORRECT, ApiConstants.TOTAL_INCORRECT_COUNT);
		sessionActivityMetrics.put(ApiConstants._ANSWER_OBJECT,ApiConstants.ANSWER_OBJECT);
		sessionActivityMetrics.put(ApiConstants.OPTIONS, ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.A.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.B.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.C.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.D.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.E.option(), ApiConstants.OPTIONS);
		sessionActivityMetrics.put(options.F.option(), ApiConstants.OPTIONS);
	}
	
	public static String getSessionActivityMetricsMapValue(String key) {
		return sessionActivityMetrics.containsKey(key) ? sessionActivityMetrics.get(key) : null;
	}
	
	public static Map<String, String> getSessionActivityMetricsMap() {
		return sessionActivityMetrics;
	}

	public static Map<String,String> getStringColumns() {
		return stringColumns;
	}
	public static Map<String,String> getLongColumns() {
		return longColumns;
	}

	public static Set<String> getStudentsCollectionUsageColumnSuffix() {
		return studentsCollectionUsageColumnSuffix;
	}

	public static void setStudentsCollectionUsageColumnSuffix(
			Set<String> studentsCollectionUsageColumnSuffix) {
		DataUtils.studentsCollectionUsageColumnSuffix = studentsCollectionUsageColumnSuffix;
	}

	public static Collection<String> getCollectionSummaryResourceColumns() {
		return collectionSummaryResourceColumns;
	}

	public static void setCollectionSummaryResourceColumns(
			Collection<String> collectionSummaryResourceColumns) {
		DataUtils.collectionSummaryResourceColumns = collectionSummaryResourceColumns;
	}
	
}
