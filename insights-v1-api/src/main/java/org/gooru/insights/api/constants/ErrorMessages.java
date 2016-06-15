package org.gooru.insights.api.constants;

public final class ErrorMessages {

	public static final String E100 = "Invalid JSON!!";

	public static final String E102 = "Invalid Field in Fields:";

	public static final String E103 = "Provide mandatory fields in {0}:{1}";

	public static final String E104 = "{0} field should not be NULL or empty.";

	public static final String E105 = "Provide Valid values in {0}:{1}";

	public static final String UNHANDLED_EXCEPTION = "UnHandled read exception in {0} due to {1}";

	public static final String UNHANDLED_FIELD = "UnHandled read exception in {0} due to {1}";

	public static final String E112 = "We don't have simple text format support to _all/comma separated inputs.";

	public static final String E113 = "Invalid job name";

	private ErrorMessages() {
		throw new AssertionError();
	}
}
