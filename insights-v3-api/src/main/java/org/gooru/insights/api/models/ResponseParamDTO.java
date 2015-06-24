package org.gooru.insights.api.models;

import java.util.List;

public class ResponseParamDTO<M> {

	private List<M> content;

	private M message;

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

}
