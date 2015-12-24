package org.gooru.insights.api.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsightsLogger {

	private static final Logger logger = LoggerFactory.getLogger(InsightsLogger.class);

	private static final String TRACE_ID = "traceId: ";

	private static final String MESSAGE = "message: ";

	private static final String COMMA = ",";
	
	public static void error(Exception exception){
		logger.error(ServiceUtils.buildString(TRACE_ID+RequestUtils.getTraceId()),exception);
	}
	
	public static void error(String msg,Exception exception){
		logger.error(getTraceMessage(msg),exception);
	}
	
	public static void error(String msg){
		logger.error(getTraceMessage(msg));
	}
	
	public static void debug(Exception exception){
		logger.debug(ServiceUtils.buildString(TRACE_ID+RequestUtils.getTraceId()),exception);
	}
	
	public static void debug(String msg){
		logger.debug(getTraceMessage(msg));
	}
	
	public static void debug(String msg,Exception exception){
		logger.debug(getTraceMessage(msg),exception);
	}
	
	public static void info(String msg){
		logger.debug(getTraceMessage(msg));
	}
	
	private static String getTraceMessage(String message){
		return ServiceUtils.buildString(TRACE_ID,RequestUtils.getTraceId(),COMMA,MESSAGE,message);
	}
}
