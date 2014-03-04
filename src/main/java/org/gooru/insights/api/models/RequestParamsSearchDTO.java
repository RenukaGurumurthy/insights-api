/*******************************************************************************
 * RequestParamsSearchDTO.java
 * insights-read-api
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.gooru.insights.api.models;

public class RequestParamsSearchDTO {
	
	
	private Integer pageNum;
	
	private Integer pageSize;
	
	private String query;
	
	private String category;
	
	private String subjectName;
	
	private String attribution;
	
	private String grade;
	
	private String license;
	
	private String standard;
	
	private String cfLicenseTagType;
	
	private String cfAggregateAttribution;
	
	private String cfParentAttribution;
	
	private String facet;
	
	private String userDetails;
	
	private String sessionToken;
	
	private String taxonomyGrade;
	
	private String owner;
	
	private String gooruOId;
	
	public String getGooruOId() {
		return gooruOId;
	}

	public void setGooruOId(String gooruOId) {
		this.gooruOId = gooruOId;
	}

	public Integer getPageNum() {
		return pageNum;
	}

	public void setPageNum(Integer pageNum) {
		this.pageNum = pageNum;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getSubjectName() {
		return subjectName;
	}

	public void setSubjectName(String subjectName) {
		this.subjectName = subjectName;
	}

	public String getAttribution() {
		return attribution;
	}

	public void setAttribution(String attribution) {
		this.attribution = attribution;
	}

	public String getGrade() {
		return grade;
	}

	public void setGrade(String grade) {
		this.grade = grade;
	}

	public String getLicense() {
		return license;
	}

	public void setLicense(String license) {
		this.license = license;
	}

	public String getStandard() {
		return standard;
	}

	public void setStandard(String standard) {
		this.standard = standard;
	}

	public String getCfLicenseTagType() {
		return cfLicenseTagType;
	}

	public void setCfLicenseTagType(String cfLicenseTagType) {
		this.cfLicenseTagType = cfLicenseTagType;
	}

	public String getCfAggregateAttribution() {
		return cfAggregateAttribution;
	}

	public void setCfAggregateAttribution(String cfAggregateAttribution) {
		this.cfAggregateAttribution = cfAggregateAttribution;
	}

	public String getCfParentAttribution() {
		return cfParentAttribution;
	}

	public void setCfParentAttribution(String cfParentAttribution) {
		this.cfParentAttribution = cfParentAttribution;
	}

	public String getFacet() {
		return facet;
	}

	public void setFacet(String facet) {
		this.facet = facet;
	}

	public String getUserDetails() {
		return userDetails;
	}

	public void setUserDetails(String userDetails) {
		this.userDetails = userDetails;
	}

	public String getSessionToken() {
		return sessionToken;
	}

	public void setSessionToken(String sessionToken) {
		this.sessionToken = sessionToken;
	}

	public String getTaxonomyGrade() {
		return taxonomyGrade;
	}

	public void setTaxonomyGrade(String taxonomyGrade) {
		this.taxonomyGrade = taxonomyGrade;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	
	
}
