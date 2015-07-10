package org.gooru.insights.api.utils;

import java.util.HashMap;
import java.util.Map;

import org.gooru.insights.api.constants.ApiConstants;

public class DataUtils {

	private static Map<String,String> assessmentAnswerSelect;

	static {
		assessmentAnswerSelect = new HashMap<String, String>();
		assessmentAnswerSelect.put("collection_gooru_oid", "collectionGooruOId");
		assessmentAnswerSelect.put("is_correct", "isCorrect");
		assessmentAnswerSelect.put("question_gooru_oid", "gooruOId");
		assessmentAnswerSelect.put("sequence", "sequence");
		assessmentAnswerSelect.put("answer_text", "text");
		assessmentAnswerSelect.put("question_type", "questionType");
		assessmentAnswerSelect.put("type_name", "type");
	}
	
	private static Map<String,String> sessionActivityMetrics;

	static {
		sessionActivityMetrics = new HashMap<String, String>();
		sessionActivityMetrics.put(ApiConstants._COLLECTION_TYPE, ApiConstants.COLLECTION_TYPE);
		sessionActivityMetrics.put(ApiConstants.VIEWS, ApiConstants.VIEWS);
		sessionActivityMetrics.put(ApiConstants._SCORE_IN_PERCENTAGE, ApiConstants.SCORE_IN_PERCENTAGE);
		sessionActivityMetrics.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		sessionActivityMetrics.put(ApiConstants.RA, ApiConstants.REACTION);
		sessionActivityMetrics.put(ApiConstants.CHOICE, ApiConstants.TEXT);
		sessionActivityMetrics.put(ApiConstants.TYPE, ApiConstants.QUESTION_TYPE);
		sessionActivityMetrics.put(ApiConstants._FEEDBACK_PROVIDER, ApiConstants.FEEDBACKPROVIDER);
		sessionActivityMetrics.put(ApiConstants._QUESTION_STATUS, ApiConstants.STATUS);
		sessionActivityMetrics.put(ApiConstants.SCORE, ApiConstants.SCORE);
		sessionActivityMetrics.put(ApiConstants.TAU, ApiConstants.TOTAL_ATTEMPT_USER_COUNT);
		sessionActivityMetrics.put(ApiConstants.SKIPPED, ApiConstants.SKIPPED);
		sessionActivityMetrics.put("attempts", "attempts");
		sessionActivityMetrics.put("correct", "totalCorrectCount");
		sessionActivityMetrics.put("in_correct", "totalInCorrectCount");
		sessionActivityMetrics.put("answer_object", "answerObject");
		sessionActivityMetrics.put("options", "options");
		sessionActivityMetrics.put("A", "options");
		sessionActivityMetrics.put("B", "options");
		sessionActivityMetrics.put("C", "options");
		sessionActivityMetrics.put("D", "options");
		sessionActivityMetrics.put("E", "options");
		sessionActivityMetrics.put("F", "options");
		sessionActivityMetrics.put("G", "options");
		sessionActivityMetrics.put("H", "options");
	}
	
	private static Map<String,String> sessionActivityMetricsDataType;

	static {
		sessionActivityMetricsDataType = new HashMap<String, String>();
		sessionActivityMetricsDataType.put(ApiConstants._COLLECTION_TYPE, "String");
		sessionActivityMetricsDataType.put(ApiConstants.VIEWS, ApiConstants.VIEWS);
		sessionActivityMetricsDataType.put(ApiConstants._SCORE_IN_PERCENTAGE, ApiConstants.SCORE_IN_PERCENTAGE);
		sessionActivityMetricsDataType.put(ApiConstants._TIME_SPENT, ApiConstants.TIMESPENT);
		sessionActivityMetricsDataType.put(ApiConstants.RA, ApiConstants.REACTION);
		sessionActivityMetricsDataType.put(ApiConstants.CHOICE, ApiConstants.TEXT);
		sessionActivityMetricsDataType.put(ApiConstants.TYPE, ApiConstants.QUESTION_TYPE);
		sessionActivityMetricsDataType.put(ApiConstants._FEEDBACK_PROVIDER, ApiConstants.FEEDBACKPROVIDER);
		sessionActivityMetricsDataType.put(ApiConstants._QUESTION_STATUS, ApiConstants.STATUS);
		sessionActivityMetricsDataType.put(ApiConstants.SCORE, ApiConstants.SCORE);
		sessionActivityMetricsDataType.put(ApiConstants.TAU, ApiConstants.TOTAL_ATTEMPT_USER_COUNT);
		sessionActivityMetricsDataType.put(ApiConstants.SKIPPED, ApiConstants.SKIPPED);
		sessionActivityMetricsDataType.put("attempts", "attempts");
		sessionActivityMetricsDataType.put("correct", "totalCorrectCount");
		sessionActivityMetricsDataType.put("in_correct", "totalInCorrectCount");
		sessionActivityMetricsDataType.put("answer_object", "answerObject");
		sessionActivityMetricsDataType.put("options", "options");
		sessionActivityMetricsDataType.put("~A", "options");
		sessionActivityMetricsDataType.put("~B", "options");
		sessionActivityMetricsDataType.put("~C", "options");
		sessionActivityMetricsDataType.put("~D", "options");
		sessionActivityMetricsDataType.put("~E", "options");
		sessionActivityMetricsDataType.put("~F", "options");
		sessionActivityMetricsDataType.put("~G", "options");
		sessionActivityMetricsDataType.put("~H", "options");
	}
	
	public static String getAssessmentAnswerSelect(String key) {
		return assessmentAnswerSelect.containsKey(key) ? assessmentAnswerSelect.get(key) : null;
	}
	
	public static String getSessionActivityMetricsMapValue(String key) {
		return sessionActivityMetrics.containsKey(key) ? sessionActivityMetrics.get(key) : null;
	}
	
	public static Map<String, String> getSessionActivityMetricsMap() {
		return sessionActivityMetrics;
	}

}
