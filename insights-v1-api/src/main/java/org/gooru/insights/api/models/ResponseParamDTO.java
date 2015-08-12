package org.gooru.insights.api.models;

import java.util.List;
import java.util.Map;

import org.apache.http.HttpStatus;

public class ResponseParamDTO<M> {

	private List<M> content;

	private M message;

	private Map<String, Object> paginate;

	private Integer statusCode;
	
	public List<M> getContent() {
		return content;
	}

	public void setContent(List<M> content) {
		this.content = content;
	}

	public M getMessage() {
		return message;
	}

	public void setMessage(M message) {
		this.message = message;
	}

	public Map<String, Object> getPaginate() {
		return paginate;
	}

	public void setPaginate(Map<String, Object> paginate) {
		this.paginate = paginate;
	}

	public Integer getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(Integer statusCode) {
		this.statusCode = statusCode;
	}
}
