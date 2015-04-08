package org.gooru.insights.api.models;

import java.io.Serializable;
import java.util.Map;

public class RequestParamsDTO implements Serializable {

	private static final long serialVersionUID = -2840599796987757919L;

	public Map<Integer,String> message;
	
	private String fields;
	
	private String fileName;
	
	private RequestParamsPaginationDTO paginate;
	
	private RequestParamsFiltersDTO filters;
	
	private String groupBy;

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	public RequestParamsFiltersDTO getFilters() {
		return filters;
	}

	public void setFilters(RequestParamsFiltersDTO filters) {
		this.filters = filters;
	}

	public String getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	public RequestParamsPaginationDTO getPaginate() {
		return paginate;
	}

	public void setPaginate(RequestParamsPaginationDTO paginate) {
		this.paginate = paginate;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

}
