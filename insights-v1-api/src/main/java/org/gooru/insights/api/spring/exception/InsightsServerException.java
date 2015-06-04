package org.gooru.insights.api.spring.exception;

public class InsightsServerException extends RuntimeException {

	private static final long serialVersionUID = 4525654760966310975L;

	public InsightsServerException() {
		super();
	}

	// Overloaded Constructor for preserving the Message
	public InsightsServerException(String msg) {
		super(msg);
	}

	// Overloaded Constructor for preserving the Message & cause
	public InsightsServerException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
