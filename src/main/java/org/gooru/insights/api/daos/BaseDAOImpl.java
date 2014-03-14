/*******************************************************************************
 * BaseDAOImpl.java
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
package org.gooru.insights.api.daos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.RequestParamsDTO;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class BaseDAOImpl implements BaseDAO,InsightsConstant {

	@Autowired
	@javax.annotation.Resource(name = "sessionFactory")
	private SessionFactory sessionFactory;

	public String getTimeInHour(String time) {

		String sql = "select hour from dim_time  where time_id='" + time + "' ";
		return getSession().getCurrentSession().createSQLQuery(sql).list().get(0).toString();
	}

	public String getDateValue(String timeCode) {

		String sql = "select date  from dim_date where date_id = '" + timeCode + "' ";
		return getSession().getCurrentSession().createSQLQuery(sql).list().get(0).toString();

	}

	public String getTodayDateId(String date) {

		String sql = "select date_id  from dim_date where day = date_format('" + date + "','%D') and year=date_format('" + date + "','%Y') and month=month('" + date + "') limit 1";
		return getSession().getCurrentSession().createSQLQuery(sql).list().get(0).toString();

	}

	public String getWeekDateId(String date) {

		String sql = "select date_id  from dim_date where year=date_format('" + date + "','%Y') and yearweek_week=week('" + date + "') limit 1";
		return getSession().getCurrentSession().createSQLQuery(sql).list().get(0).toString();
	}

	public String getDate(String dateId) {

		String sql = "select date  from dim_date where date_id = '" + dateId + "'";
		return getSession().getCurrentSession().createSQLQuery(sql).list().get(0).toString();

	}

	public String getMonthDateId(String date) {

		String sql = "select date_id  from dim_date where year=date_format('" + date + "','%Y') and month=month('" + date + "') limit 1";
		return getSession().getCurrentSession().createSQLQuery(sql).list().get(0).toString();

	}

	public String getQuaterDateId(String date) {

		String sql = "select date_id,date  from dim_date where year=date_format('" + date + "','%Y') and month <=month('" + date + "') group by quarteryear order by month DESC limit 1";
		return getSession().getCurrentSession().createSQLQuery(sql).list().get(0).toString();

	}

	public String getYearDateId(String date) {

		String sql = "select date_id  from dim_date where year=date_format('" + date + "','%Y')  limit 1";
		return getSession().getCurrentSession().createSQLQuery(sql).list().get(0).toString();

	}

	public String getAllDateId(String sortOrder) {

		String sql = "select date  from dim_date where month =01 AND day =01 AND year<=DATE_FORMAT(now(),'%Y') ORDER BY date "+sortOrder+" limit 1";
		return getSession().getCurrentSession().createSQLQuery(sql).list().get(0).toString();

	}

	public void addWhereClause(StringBuffer updatedWhereClauseSb, String clauseToAdd) {
		if (updatedWhereClauseSb != null && updatedWhereClauseSb.length() != 0) {
			updatedWhereClauseSb.append(" AND ");
		}
		updatedWhereClauseSb.append(clauseToAdd);
	}

	public SessionFactory getSession() {

		return sessionFactory;
	}

	public List<Map<String, Object>> getListFromObject(List<Object[]> object, String selectFields) {

		String selectedArray[] = selectFields.split(",");

		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Object[] resultValues : object) {
			Map<String, Object> resultObject = new HashMap<String, Object>();
			for (int i = 0; i < selectedArray.length; i++) {
				if(resultValues[i] != null){
				resultObject.put(selectedArray[i].toString(), resultValues[i]);
			}
			}
			resultList.add(resultObject);
		}

		return resultList;
	}
	public List<Map<String, String>> getListFromObject(List<Object[]> object, String[] selectFields,String startDate,String endDate) {

		List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
		for (Object[] resultValues : object) {
			Map<String, String> resultObject = new LinkedHashMap<String, String>();
			addDate(startDate,endDate,resultObject);
			for (int i = 0; i < selectFields.length; i++) {
				if(resultValues[i] != null){
				resultObject.put(selectFields[i].toString(), resultValues[i].toString());
			}
			}
			resultList.add(resultObject);
		}

		return resultList;
	}

	public List<Map<String, Object>> getListFromData(List object, String selectFields) {

		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (int i = 0; i < object.size(); i++) {
			Map<String, Object> resultObject = new HashMap<String, Object>();
			if(object.get(i) != null){
			resultObject.put(selectFields, object.get(i).toString());
			}
			resultList.add(resultObject);
		}
		return resultList;
	}

	public List<Map<String, Object>> executeQuery(String sql, String selectField, Integer totalSelectFields, String requestFields, String InvalidData,String  unixStartDate,String unixEndDate,boolean paginate, String sqlPaginate,
			Map<String, String> errorMessage, String orderBy, Map<String, Map<String, Integer>> paramList,List<Map<String,String>> errorData) {

		Session session = sessionFactory.getCurrentSession();
		List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
	
		if (checkNull(InvalidData)) {
			errorMessage.put("checkDocument", InvalidData);
		}
		if (totalSelectFields > 1) {
			Query query = session.createSQLQuery(sql);
			setParamList(paramList, query);
			List<Object[]> object = query.list();
			resultSet = getListFromObject(object, requestFields);
			if(resultSet.isEmpty()){
				errorMessage = new HashMap<String, String>();
				errorMessage.put(checkFilter.KEY.getFilterStatus(), checkFilter.VALUE.getFilterStatus());
			}
			errorMessage.put("unixStartDate",unixStartDate );
			errorMessage.put("unixEndDate", unixEndDate);

		} else if (totalSelectFields == 1) {
			Query query = session.createSQLQuery(sql);
			setParamList(paramList, query);
			List<Object> object = query.list();
			resultSet = getListFromData(object, requestFields);
			if(resultSet.isEmpty()){
				errorMessage = new HashMap<String, String>();
				errorMessage.put(checkFilter.KEY.getFilterStatus(),checkFilter.VALUE.getFilterStatus());
			}
			errorMessage.put("unixStartDate",unixStartDate );
			errorMessage.put("unixEndDate", unixEndDate);
		}

	
		if (paginate) {
			Map<String, String> totalRows = new HashMap<String, String>();
			Query query = session.createSQLQuery(sqlPaginate);
			setParamList(paramList, query);
			Integer totalSet =  query.list().size();
			errorMessage.put("totalRows",String.valueOf(totalSet));

		}

		if (!errorMessage.isEmpty()) {
			errorData.add(errorMessage);
		}

		return resultSet;
	}
	
	public void setParamList(Map<String, Map<String, Integer>> paramList, Query query) {
		
		for (Map.Entry<String, Map<String, Integer>> entry : paramList.entrySet())
		{
			
			for (Entry<String, Integer> paramEntry : entry.getValue().entrySet())
			{
				if(paramEntry.getValue() == 1){
					query.setParameterList(entry.getKey(), paramEntry.getKey().split(","));
				}
			}
		}
	}

	

	public String checkSortBy(String requestFields, String sortBy, Map<String, String> errorMessage) {

		String response = "";
		boolean firstEntry = false;
		boolean checkedField = false;
		String field = "";
		for (String check : requestFields.split(",")) {
			
			for(String back : sortBy.split(",")){
			
				
				if (back.equalsIgnoreCase(check)) {
					
					if(firstEntry){
						response += ","+back;
					}else if(!firstEntry){
						response += back;
					}
					 firstEntry = true;
				}
			}
			
		}
		if (checkNull(response)) {
			if(response.length() != sortBy.length()){
				errorMessage.put("paginate:sortBy", " Invalid data in sortBy parameter ("+sortBy+") ");
			}
		} else {
			errorMessage.put("paginate:sortBy", " please give the acceptable field in sortBy and that field should be " + " given in fields:" + sortBy + "");
		}
		return response;
	}

	public List<Map<String, Object>> processRecords(RequestParamsDTO requestParamsDTO, String sql, String selectFields, String InvalidFields, String requestFields, Integer totalFields,String unixStartDate,String unixEndDate,String groupBy,Map<String, Map<String, Integer>> paramList,List<Map<String,String>> errorData) {

		Map<String, String> errorMessage = new HashMap<String, String>();

		String sqlData = " SELECT " + selectFields + " " + sql;
		String sqlPaginate = "";
		boolean paginate = false;

		String orderBy = null;
		if(requestParamsDTO.getGroupBy() != null){
			String checkGroupBy = checkGroupBy(requestFields,requestParamsDTO.getGroupBy(),errorMessage);

			if(checkNull(checkGroupBy)){
				sqlData +=" GROUP BY "+checkGroupBy+" ";
				sqlPaginate = " SELECT "+selectFields+" " + sql;
				sqlPaginate +=" GROUP BY "+checkGroupBy+" ";				
			}else{
				sqlData +=groupBy;
				sqlPaginate = " SELECT 1 " + sql;
				sqlPaginate +=groupBy;
			}
			
		}else{
			sqlData +=groupBy;
			sqlPaginate = " SELECT 1 " + sql;
			sqlPaginate +=groupBy;
		}
		if(requestParamsDTO.getPaginate() != null){
		if (checkNull(requestParamsDTO.getPaginate().getSortBy())) {
			String sortBy = requestParamsDTO.getPaginate().getSortBy();
			orderBy = checkSortBy(requestFields, sortBy, errorMessage);
			if (checkNull(orderBy)) {

				sqlData += " ORDER BY  " + orderBy + " ";
			

			if (checkNull(requestParamsDTO.getPaginate().getSortOrder())) {
				if ((requestParamsDTO.getPaginate().getSortOrder()).equalsIgnoreCase("DESC") | (requestParamsDTO.getPaginate().getSortOrder()).equalsIgnoreCase("ASC")) {

					sqlData += " " + requestParamsDTO.getPaginate().getSortOrder() + " ";

				} else {
					errorMessage.put(checkSortOrder.KEY.getSortOrder(), checkSortOrder.VALUE.getSortOrder());
					sqlData += " ASC ";
				}

			} else {
				sqlData += " ASC ";
			}
			}
		}
		if (checkNull(requestParamsDTO.getPaginate().getLimit()) || checkNull(requestParamsDTO.getPaginate().getOffset()) || checkNull(requestParamsDTO.getPaginate().getTotalRecords())) {

			paginate = true;
		}else{
			errorMessage.put(requestParameter.KEY.getRequestParameter(),requestParameter.VALUE.getRequestParameter());
		}
		Integer offset = checkNull(requestParamsDTO.getPaginate().getOffset()) ? requestParamsDTO.getPaginate().getOffset() : 0;

		if (offset != 0) {
			offset = offset - 1;
		}
		Integer Limit = checkNull(requestParamsDTO.getPaginate().getLimit()) ? requestParamsDTO.getPaginate().getLimit() : 10;
		Integer startRow = (offset * Limit);

		if (checkNull(requestParamsDTO.getPaginate().getTotalRecords())) {
			Integer requestedTotalRows = requestParamsDTO.getPaginate().getTotalRecords();

			if(startRow < requestedTotalRows ){
			if (Limit < requestedTotalRows) {

				if (paginate) {

					sqlPaginate += " LIMIT 0 , " + requestedTotalRows + "";
				}
				sqlData += " LIMIT " + startRow + " , " + Limit + "";

			} else if (Limit >= requestedTotalRows) {

				if (paginate) {
					sqlPaginate += " LIMIT 0 , " + requestedTotalRows + "";
				}
				startRow = 0;
				sqlData += " LIMIT " + startRow + " , " + requestedTotalRows + "";
				
			}
			}else if(startRow >= requestedTotalRows){
				
				if (Limit < requestedTotalRows) {

					if (paginate) {

						sqlPaginate += " LIMIT 0 , " + requestedTotalRows + "";
					}
					sqlData += " LIMIT " + requestedTotalRows + " , " + Limit + "";

				} else if (Limit >= requestedTotalRows) {

					if (paginate) {
						sqlPaginate += " LIMIT 0 , " + requestedTotalRows + "";
					}
					startRow = 0;
					sqlData += " LIMIT " + requestedTotalRows + " , " + requestedTotalRows + "";

				}
			}
		} else {

			sqlData += " LIMIT " + startRow + " , " + Limit + "";
		}
		}else{
			sqlData += " ";
		}
		

		
		return executeQuery(sqlData, selectFields, totalFields, requestFields, InvalidFields,unixStartDate,unixEndDate, paginate, sqlPaginate, errorMessage, orderBy, paramList,errorData);
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
	
	public void addDate(String startDate,String endDate,Map<String,String> resultObject){
		
		if(checkNull(startDate) && checkNull(endDate)){
			
			resultObject.put("startDate", startDate);
			resultObject.put("endDate", endDate);
		
		}else if(checkNull(startDate)){
			
			resultObject.put("startDate", startDate);
			resultObject.put("endDate", startDate);
		}else{
			
			resultObject.put("Date", "all");

		}
		
	}
	
	  public String checkGroupBy(String requestFields, String groupBy, Map<String, String> errorMessage) {

			String response = "";
			boolean firstEntry = false;
			for (String check : requestFields.split(",")) {
				
				for(String back : groupBy.split(",")){
				
					
					if (back.equalsIgnoreCase(check)) {
						
						if(firstEntry){
							response += ","+back;
						}else if(!firstEntry){
							response += back;
						}
						 firstEntry = true;
					}
				}
			}
			if (checkNull(response)) {
				if(response.length() != groupBy.length()){
					errorMessage.put("groupBy", " Invalid data in group By parameter ("+groupBy+") ");
				}
			} else {
				errorMessage.put("groupBy", " please give the acceptable field in groupBy and that field should be " + " given in fields:" + groupBy + "");
			}
			return response;
		}

}
