package org.gooru.insights.api.services;

public enum CqlQueries {

	GET_SESSION_ACTIVITY("SELECT event_type FROM user_session_activity WHERE session_id=? AND gooru_oid=?");

	private String query;

	private CqlQueries(String CqlQueries) {
		query = CqlQueries;
	}

	public String getQuery() {
		return query;
	}
}
