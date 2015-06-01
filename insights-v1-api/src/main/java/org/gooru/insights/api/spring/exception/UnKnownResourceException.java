package org.gooru.insights.api.spring.exception;

public class UnKnownResourceException extends RuntimeException{

	private static final long serialVersionUID = 4525654760966310972L;

	public UnKnownResourceException() {
		super();
	}

	// Overloaded Constructor for preserving the Message
	public UnKnownResourceException(String msg) {
		super(msg);
	}

	// Overloaded Constructor for preserving the Message & cause
	public UnKnownResourceException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
