package org.gooru.insights.api.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InsightsLogger {

	private static final Logger logger = LoggerFactory.getLogger(InsightsLogger.class);

	private InsightsLogger() {
		throw new AssertionError();
	}

	public static void error(Exception exception){
		logger.error(exception.getMessage());
	}

	public static void error(String msg,Exception exception){
		logger.error(msg,exception);
	}

	public static void error(String msg){
		logger.error(msg);
	}

	public static void debug(Exception exception){
		logger.debug(exception.getMessage());
	}

	public static void debug(String msg){
		logger.debug(msg);
	}

	public static void debug(String msg,Exception exception){
		logger.debug(msg,exception);
	}

	public static void info(String msg){
		logger.debug(msg);
	}

}
