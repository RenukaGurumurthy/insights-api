/*******************************************************************************
 * InsightsConstant.java
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

public interface InsightsConstant {

	public static final String DATA_OBJECT = "data";
	
	public static final String DAY ="day";
	
	public static final String WEEK ="week";
	
	public static final String MONTH ="month";
	
	public static final String YEAR ="year";
	
	public static final String ALL ="all";
	
	public static final String WORLD ="world";
	
	public static final String USER ="user";
	
	public static final String HOUR ="hour";
	
	public static final String QUATER = "quater";
	
	public static final String ZERO_ONE ="01";
	
	public static final String FILTER_AGGREGATE="filterAggregate";
	
	public static final String TWENTY_THOUSAND="20000";
	
	public static final String GOORU_TAXONOMY ="Gooru Taxonomy";
	
	public static final Integer THREE_SIXTY_FIVE =365;
	
	public static final String START_DATE = "startDate";
	
	public static final String END_DATE = "endDate";
	
	public static final String START_DATE_ID = "startDateId";
	
	public static final String END_DATE_ID = "endDateId";
	
	public static final String TABLE_TYPE = "tableType";
	
	public static final String TAXONOMY = "taxonomy";
	
	public static final String PARTNER_ID = "partnerId";
	
	public static final String PARTNER = "partner";
	
	public static final String EMAIL_ID = "emailId";
	
	public static final String SUBJECT = "Insights Admin Report";
	
	public static final String EVENT_DETAIL_CF ="event_detail";
	
	public static final String EVENT_TIMELINE_CF ="event_timeline";
	
	public static final String MAIL_BODY ="Hi, " +
			" Please download the gooru ";
	
	public enum checkJson{
		KEY("requestJSON"), VALUE("Invalid JSON Format");
		
		private String jsonStatus;
		
		private checkJson(String value){
			
			jsonStatus = value;
		}
		
		public String getJson(){
			return jsonStatus;
		}
	}
	
	public enum checkFields{
		KEY("ReferDocument"),VALUE("Hi,There is no request to retrive data,Give valid data in fields");
		
		private String fieldsStatus;
		
		private checkFields(String value){
			fieldsStatus = value;
		}
		
		public String getFields(){
			return fieldsStatus;
		}
	}
	
	public enum validProcess{
		KEY("proceed"),VALUE("Data are checked");
		private String processStatus;
		private validProcess(String value){
			processStatus = value;
		}
		
		public String getProcess(){
			return processStatus;
		}
		
	}
	
	public enum checkDataType{
		KEY("invalidDatatype"),VALUE("kindly check the request variables datatype");
		
		private String dataTypeStatus;
		private checkDataType(String value){
			dataTypeStatus = value;
			
		}
		public String getDataType(){
			return dataTypeStatus;
		}
	}
	
	public enum checkFilter{
		KEY("TryAgain"),VALUE("Sorry,There is no data available for this filter better try different filters,Thanks!!!");
		
		private String filterStatus;
		private checkFilter(String value){
			filterStatus = value;
		}
		public String getFilterStatus(){
			return filterStatus;
		}
		
	}
	
	public enum checkSortOrder{
		KEY("sortOrder"),VALUE("please give ASC for ascending and DESC for descending");
		
		private String sortOrder;
		private checkSortOrder(String value){
			sortOrder = value;
		}
		public String getSortOrder(){
			return sortOrder;
		}
	}
	
	public enum requestParameter{
		KEY("requestParameter"),VALUE("I need atleast offset and limit  or offset and totalRecords parameter values,default limit is 10.");
	
		private String requestParameterStatus;
		
		private requestParameter(String value){
			requestParameterStatus = value;
		}
		
		public String getRequestParameter(){
			return requestParameterStatus;
		}
	}
	
}
