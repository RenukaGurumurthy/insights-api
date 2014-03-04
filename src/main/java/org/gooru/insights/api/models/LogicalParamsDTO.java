/*******************************************************************************
 * LogicalParamsDTO.java
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

public class LogicalParamsDTO {
	

	String taxonomy;
	
	String startDateId;
	
	String endDateId;
	
	String filterAggregate;
	
	String tableType;
	
	long unixStartDate;
	
	long unixEndDate;

	public String getTaxonomy() {
		return taxonomy;
	}

	public void setTaxonomy(String taxonomy) {
		this.taxonomy = taxonomy;
	}

	public String getStartDateId() {
		return startDateId;
	}

	public void setStartDateId(String startDateId) {
		this.startDateId = startDateId;
	}

	public String getEndDateId() {
		return endDateId;
	}

	public void setEndDateId(String endDateId) {
		this.endDateId = endDateId;
	}

	public String getFilterAggregate() {
		return filterAggregate;
	}

	public void setFilterAggregate(String filterAggregate) {
		this.filterAggregate = filterAggregate;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public long getUnixStartDate() {
		return unixStartDate;
	}

	public void setUnixStartDate(long unixStartDate) {
		this.unixStartDate = unixStartDate;
	}

	public long getUnixEndDate() {
		return unixEndDate;
	}

	public void setUnixEndDate(long unixEndDate) {
		this.unixEndDate = unixEndDate;
	}
	
}
