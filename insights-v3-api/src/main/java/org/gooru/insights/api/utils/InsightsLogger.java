package org.gooru.insights.api.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsightsLogger {

	private static final Logger logger = LoggerFactory.getLogger(InsightsLogger.class);

	private static final String TRACE_ID = "traceId: ";

	private static final String MESSAGE = "message: ";

	private static final String COMMA = ",";
	
	public static void error(String traceId,Exception exception){
		logger.error(TRACE_ID+traceId,exception);
	}
	
	public static void error(String traceId,String msg,Exception exception){
		logger.error(TRACE_ID+traceId+COMMA+MESSAGE+msg,exception);
	}
	
	public static void error(String traceId,String msg){
		logger.error(TRACE_ID+traceId+COMMA+MESSAGE+msg);
	}
	
	public static void debug(String traceId,Exception exception){
		logger.debug(TRACE_ID+traceId,exception);
	}
	
	public static void debug(String traceId,String msg){
		logger.debug(TRACE_ID+traceId+COMMA+MESSAGE+msg);
	}
	
	public static void debug(String traceId,String msg,Exception exception){
		logger.debug(TRACE_ID+traceId+COMMA+MESSAGE+msg,exception);
	}
	
	public static void info(String traceId,String msg){
		logger.debug(TRACE_ID+traceId+COMMA+MESSAGE+msg);
	}
}
