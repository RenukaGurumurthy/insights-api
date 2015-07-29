package org.gooru.insights.api.models;

import java.io.Serializable;

public class JobStatus implements Serializable {

	/**
	 * @author daniel
	 */
	private static final long serialVersionUID = 1508059504085582662L;

	private String queue;
	
	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

}

