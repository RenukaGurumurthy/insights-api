/*******************************************************************************
 * BaseServiceImpl.java
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
package org.gooru.insights.api.services;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.daos.BaseDAO;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.LogicalParamsDTO;
import org.gooru.insights.api.models.RequestParamsDTO;
import org.gooru.insights.api.utils.JsonDeserializer;
import org.gooru.insights.api.utils.Serializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@Service
public class BaseServiceImpl extends Serializer implements InsightsConstant,BaseService {

	private static final Logger logger = LoggerFactory.getLogger(BaseServiceImpl.class);

	@Autowired
	private SelectParamsService selectParamsService;

	@Autowired
	private BaseDAO baseDAO;

	private LogicalParamsDTO logicalParamsDTO;
	
	public boolean checkAggregate(String aggregate){
		String a[] = {"all","year","week","month","day"};
		boolean validAggregate = false;
		for(int i =0 ; i< a.length;i++){
			if(a[i].equalsIgnoreCase(aggregate)){
				validAggregate = true;
			}
		}
		return validAggregate;
	}

	public LogicalParamsDTO getRequestParameters(RequestParamsDTO requestParamsDTO) throws ParseException {

		LogicalParamsDTO logicalParamsDTO = new LogicalParamsDTO();

		if (requestParamsDTO != null) {

			Date todaysDate = new java.util.Date();
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat monthFormatter = new SimpleDateFormat("yyyy-MM");

			formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
			monthFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

			String date = formatter.format(todaysDate);
			Date myDate = formatter.parse(date);
			Date oneDayBefore = new Date(myDate.getTime() - 2);
			String lastDayDate = formatter.format(oneDayBefore);
			logicalParamsDTO.setTableType(WORLD);
			String dateId = null;
			String endDateId = null;
			if(requestParamsDTO.getFilters() != null){
			String processCodeId = assignValue(requestParamsDTO.getFilters().getLevelsCodeId());
			String processDepth = assignValue(requestParamsDTO.getFilters().getLevelsDepth());
			String processLabel = assignValue(requestParamsDTO.getFilters().getLevelsLabel());
			String userUId = assignValue(requestParamsDTO.getFilters().getUserUId());
			String requestStartDate = assignValue(requestParamsDTO.getFilters().getStartDate());
			String requestEndDate = assignValue(requestParamsDTO.getFilters().getEndDate());
			String filterAggregate = assignValue(requestParamsDTO.getFilters().getFilterAggregate());
			
			if(checkNull(requestStartDate) && checkNull(requestEndDate)){
				logicalParamsDTO.setFilterAggregate(DAY);
				logicalParamsDTO.setStartDateId(baseDAO.getTodayDateId(requestStartDate));
				logicalParamsDTO.setUnixStartDate(getTimeStamp(requestStartDate));
				logicalParamsDTO.setEndDateId(baseDAO.getTodayDateId(requestEndDate));
				logicalParamsDTO.setUnixEndDate(getTimeStamp(requestEndDate));

			}else{

				if(filterAggregate != null && filterAggregate != "" && (!filterAggregate.isEmpty())){
			if (!filterAggregate.equalsIgnoreCase(ALL)) {

				if (filterAggregate.equalsIgnoreCase(DAY)) {
					logicalParamsDTO.setFilterAggregate(HOUR);
					logicalParamsDTO.setStartDateId(lastDayDate);
					logicalParamsDTO.setUnixStartDate(getTimeStamp(baseDAO.getDateValue(lastDayDate)));

				} else if (filterAggregate.equalsIgnoreCase(WEEK)) {
					logicalParamsDTO.setFilterAggregate(DAY);
					endDateId = baseDAO.getTodayDateId(date);
					logicalParamsDTO.setStartDateId(String.valueOf((Integer.parseInt(endDateId) - 7)));
					logicalParamsDTO.setUnixStartDate(getTimeStamp(baseDAO.getDateValue(String.valueOf((Integer.parseInt(endDateId) - 7)))));
					logicalParamsDTO.setEndDateId(String.valueOf(((Integer.parseInt(endDateId)) - 1)));
					logicalParamsDTO.setUnixEndDate(getTimeStamp(baseDAO.getDateValue(String.valueOf(((Integer.parseInt(endDateId)) - 1)))));

				} else if (filterAggregate.equalsIgnoreCase(MONTH)) {
					logicalParamsDTO.setFilterAggregate(DAY);
					endDateId = baseDAO.getTodayDateId(date);
					logicalParamsDTO.setStartDateId(String.valueOf((Integer.parseInt(endDateId) - 30)));
					logicalParamsDTO.setUnixStartDate(getTimeStamp(baseDAO.getDateValue(String.valueOf((Integer.parseInt(endDateId) - 30)))));
					logicalParamsDTO.setEndDateId(String.valueOf(((Integer.parseInt(endDateId)) - 1)));
					logicalParamsDTO.setUnixEndDate(getTimeStamp(baseDAO.getDateValue(String.valueOf(((Integer.parseInt(endDateId)) - 1)))));

				} else if (filterAggregate.equalsIgnoreCase(QUATER)) {
					logicalParamsDTO.setFilterAggregate(QUATER);
					endDateId = baseDAO.getTodayDateId(date);
					logicalParamsDTO.setStartDateId(baseDAO.getQuaterDateId(date));
					logicalParamsDTO.setUnixStartDate(getTimeStamp(baseDAO.getDateValue(baseDAO.getQuaterDateId(date))));
					logicalParamsDTO.setEndDateId(String.valueOf(((Integer.parseInt(endDateId)) - 1)));
					logicalParamsDTO.setUnixEndDate(getTimeStamp(baseDAO.getDateValue(String.valueOf(((Integer.parseInt(endDateId)) - 1)))));

				} else if (filterAggregate.equalsIgnoreCase(YEAR)) {
					logicalParamsDTO.setFilterAggregate(MONTH);
					dateId = baseDAO.getTodayDateId(date);
					Integer currDateId = Integer.parseInt(dateId);

					Calendar cal = Calendar.getInstance();
					cal.add(Calendar.MONTH, -1);
					String lastMonth = monthFormatter.format(cal.getTime());
					lastMonth = lastMonth + "-" + ZERO_ONE;
					logicalParamsDTO.setEndDateId((baseDAO.getMonthDateId(lastMonth)));
					logicalParamsDTO.setUnixEndDate(getTimeStamp(baseDAO.getDateValue(baseDAO.getMonthDateId(lastMonth))));
					Integer yearId = currDateId - THREE_SIXTY_FIVE;
					String currYear = baseDAO.getDate(String.valueOf(yearId));
					logicalParamsDTO.setStartDateId(baseDAO.getMonthDateId(currYear));
					logicalParamsDTO.setUnixStartDate(getTimeStamp(baseDAO.getDateValue(baseDAO.getMonthDateId(currYear))));

				}
			} else if (filterAggregate.equalsIgnoreCase(ALL)) {
				logicalParamsDTO.setFilterAggregate(YEAR);
				logicalParamsDTO.setUnixStartDate(getTimeStamp(baseDAO.getAllDateId("ASC")));
				logicalParamsDTO.setUnixEndDate(getTimeStamp(baseDAO.getAllDateId("DESC")));
				

			}else{
				logicalParamsDTO.setFilterAggregate(YEAR);
				logicalParamsDTO.setUnixStartDate(getTimeStamp(baseDAO.getAllDateId("ASC")));
				logicalParamsDTO.setUnixEndDate(getTimeStamp(baseDAO.getAllDateId("DESC")));
				
			}
				}else{
					
				}
			}
			if (userUId != null && (userUId != "") && (!userUId.isEmpty())) {
				logicalParamsDTO.setTableType(USER);
			}

			if (processCodeId != null && processCodeId != "" && (!processCodeId.isEmpty())) {
				if (!processCodeId.equalsIgnoreCase(TWENTY_THOUSAND)) {
					logicalParamsDTO.setTaxonomy(" subject_code_id = " + processCodeId + " ");
					if (processDepth != null && processDepth != "" && (!processDepth.isEmpty())) {

						if (processDepth.contentEquals("1")) {
							logicalParamsDTO.setTaxonomy(" subject_code_id = " + processCodeId + " ");
						} else if (processDepth.contentEquals("2")) {
							logicalParamsDTO.setTaxonomy(" course_code_id = " + processCodeId + " ");
						} else if (processDepth.contentEquals("3")) {
							logicalParamsDTO.setTaxonomy(" unit_code_id = " + processCodeId + " ");
						} else if (processDepth.contentEquals("4")) {
							logicalParamsDTO.setTaxonomy(" topic_code_id = " + processCodeId + " ");
						} else if (processDepth.contentEquals("5")) {
							logicalParamsDTO.setTaxonomy(" lesson_code_id = " + processCodeId + " ");
						}
					}
				}
			} else if (processLabel != null && processLabel != "" && (!processLabel.isEmpty())) {
				if (!processLabel.equalsIgnoreCase(GOORU_TAXONOMY)) {
					logicalParamsDTO.setTaxonomy(" subject = '" + processLabel + "' ");
					if (processDepth != null && processDepth != "" && (!processDepth.isEmpty())) {

						if (processDepth.contentEquals("1")) {
							logicalParamsDTO.setTaxonomy(" subject = '" + processLabel + "' ");
						} else if (processDepth.contentEquals("2")) {
							logicalParamsDTO.setTaxonomy(" course = '" + processLabel + "' ");
						} else if (processDepth.contentEquals("3")) {
							logicalParamsDTO.setTaxonomy(" unit = '" + processLabel + "' ");
						} else if (processDepth.contentEquals("4")) {
							logicalParamsDTO.setTaxonomy(" topic = '" + processLabel + "' ");
						} else if (processDepth.contentEquals("5")) {
							logicalParamsDTO.setTaxonomy(" lesson = '" + processLabel + "' ");
						}
					}
				}
			}
		}
		}
			
		return logicalParamsDTO;

	}

	public Map<String,String> checkJsonFormat(String data) {

		Map<String, String> incorrectFormat = new HashMap<String, String>();
		try {

			JSONObject json = requestData(data);

		} catch (Exception e) {
			
			incorrectFormat.put(checkJson.KEY.getJson(), checkJson.VALUE.getJson());
		}

		return incorrectFormat;
	}

	public static JSONObject requestData(String data) throws Exception {

		if (data != "" && data != null && (!data.isEmpty())) {
			try {
				return new JSONObject(data);
			} catch (Exception e) {

				throw new Exception(e.getMessage());

			}
		}

		return null;
	}

	public Map<String, String> getAuthenticateData(RequestParamsDTO requestParamsDTO,Map<String,String> hibernateSelectValues) throws ParseException {

		Map<String, String> checkedData = new HashMap<String, String>();
		if (checkNull(requestParamsDTO.getFields())) {
			

				
				logicalParamsDTO = getRequestParameters(requestParamsDTO);
				if(checkNull(hibernateSelectValues.get("select").toString())){
					checkedData.put(validProcess.KEY.getProcess(), validProcess.VALUE.getProcess());
					checkedData.put("filterAggregate", checkAggregate(logicalParamsDTO.getFilterAggregate())? logicalParamsDTO.getFilterAggregate() : YEAR);
					checkedData.put("startDateId", logicalParamsDTO.getStartDateId());
					checkedData.put("endDateId", logicalParamsDTO.getEndDateId());
					checkedData.put("tableType", logicalParamsDTO.getTableType());
					checkedData.put("taxonomy", logicalParamsDTO.getTaxonomy());
					checkedData.put("select",hibernateSelectValues.get("select"));
					checkedData.put("InValidParameters",hibernateSelectValues.get("InValidParameters"));
					checkedData.put("totalFields",hibernateSelectValues.get("totalFields"));
					checkedData.put("requestedValues",hibernateSelectValues.get("requestedValues"));
					checkedData.put("unixStartDate", String.valueOf(logicalParamsDTO.getUnixStartDate()));
					checkedData.put("unixEndDate", String.valueOf(logicalParamsDTO.getUnixEndDate()));

				}else
				{
					checkedData.put("checkDocument", hibernateSelectValues.get("InValidParameters"));
				}

		} else {

			checkedData.put(checkFields.KEY.getFields(), checkFields.VALUE.getFields());


		}
		return checkedData;

	}
	
	

	public static String getValue(String key, JSONObject json) throws Exception {
		try {
			if (json.isNull(key)) {
				return null;
			}
			return json.getString(key);

		} catch (JSONException e) {
			throw new Exception(e.getMessage());
		}
	}

	public RequestParamsDTO buildRequestParameters(String data) {
		return data != null ? new JsonDeserializer().deserialize(data, RequestParamsDTO.class) : null;
	}

	public JSONObject sendErrorResponse(HttpServletRequest request, HttpServletResponse response, int responseStatus, String message) throws JSONException {
		response.setStatus(responseStatus);
		response.setContentType("application/json");
		JSONObject resultJson = new JSONObject();

		resultJson.put("statusCode", responseStatus);
		resultJson.put("message", message);

		return resultJson;
	}

public boolean validate(String data,RequestParamsDTO requestParamsDTO,List<Map<String,String>> errorData){
		
		boolean dataValidate = false; 
		Map<String,String> processedData = new HashMap<String,String>();
		
		if(requestParamsDTO != null){
		
			dataValidate = true;
			
		}else{
			
				processedData.put(checkDataType.KEY.getDataType(), checkDataType.VALUE.getDataType());

				errorData.add(processedData);
		}
		
		
		return dataValidate;
	}
	
	public boolean checkData(String data,List<Map<String,String>> errorData){

		Map<String,String> setError = new HashMap<String,String>();
		setError = this.checkJsonFormat(data);
		boolean checked = false;
		
		if (setError.isEmpty()) {
			
		checked = true;
		
		return checked; 
	
		}else{
		
			errorData.add(setError);
	}
		return checked;
}
	
	public boolean canProceed(Map<String,String> processedData){
		
		if(processedData.containsKey("proceed")){
			
			return true;	
		
		}else{
		
			return false;
		}
		
	}
	
	public boolean checkNull(String parameter) {

		if (parameter != null && parameter != "" && (!parameter.isEmpty())) {

			return true;

		} else {

			return false;
		}
	}

	public boolean checkNull(Integer parameter) {

		if (parameter != null && parameter.SIZE > 0 && (!parameter.toString().isEmpty())) {

			return true;

		} else {

			return false;
		}
	}

	public String assignValue(String parameter) {
		if (parameter != null && parameter != "" && (!parameter.isEmpty())) {

			return parameter;

		} else {

			return null;
		}

	}

	public Integer assignValue(Integer parameter) {

		if (parameter != null && parameter.SIZE > 0 && (!parameter.toString().isEmpty())) {

			return parameter;

		} else {

			return null;
		}
	}

	private SelectParamsService getSelectParameter() {

		return selectParamsService;
	}

	private BaseDAO getBaseDao() {

		return baseDAO;
	}
	
	public long getTimeStamp(String Date) throws ParseException{
		if(Date != null && Date != ""){
		SimpleDateFormat dfm = new SimpleDateFormat("yyyy-MM-dd");  
		 long unixTime =0;
		 dfm.setTimeZone(TimeZone.getTimeZone("UTC"));
		 unixTime = dfm.parse(Date).getTime();  
	     unixTime=unixTime/1000;
		return unixTime;
		}else
		{
			long unixTime =0000000000;
			return unixTime;
		}
	}

	
}
