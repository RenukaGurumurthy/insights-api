/*******************************************************************************
 * CollectionRestServiceImpl.java
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.insights.api.daos.CollectionRestDAO;
import org.gooru.insights.api.models.RequestParamsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Transactional
@Service
public class CollectionRestServiceImpl implements CollectionRestService {
	
	@Autowired
	private BaseService baseService;
	
	@Autowired
	private CollectionRestDAO collectionRestDAO;
	
	@Autowired
	private SelectParamsService selectParamsService;

	@Autowired
	private CassandraService cassandraService;
	
	public List<Map<String,Object>> getCollectionData(String databaseType,String data,String collectionId,List<Map<String,String>> errorData)throws ParseException{
		Map<String,String> processedData = new HashMap<String,String>();
		Map<String,String> selectValues = new HashMap<String, String>();
		if (getBaseServiceCall().checkData(data, errorData)) {
			RequestParamsDTO requestParamsDTO = this.getBaseServiceCall()
					.buildRequestParameters(data);
			
			if(requestParamsDTO.getFilters().getDataBase() != null ? requestParamsDTO.getFilters().getDataBase().equalsIgnoreCase("mysql") : false){
				if (getBaseServiceCall().validate(data, requestParamsDTO, errorData)) {
					if (getBaseServiceCall().canProceed(processedData = getBaseServiceCall()
							.getAuthenticateData(
									requestParamsDTO,
									selectParamsService
											.getCollectionData(requestParamsDTO
													.getFields(),requestParamsDTO.getGroupBy())))) {

						return getCollectionDAO().getCollectionData(requestParamsDTO, processedData.get("filterAggregate"),processedData.get("taxonomy"), processedData.get("tableType"),
								processedData.get("startDateId"), processedData.get("endDateId"), processedData,collectionId,errorData);
						

					}

				}
				}
				else{
					boolean includeTotal = false;
					if(requestParamsDTO.getPaginate() != null){
						includeTotal = true;
					}
					Map<String,String> selectResult = new HashMap<String,String>();
					List<Map<String,Object>> resultSet = new ArrayList<Map<String,Object>>();
				if (getBaseServiceCall().validate(data, requestParamsDTO, errorData)) {
					if (getBaseServiceCall().canProceed(processedData = getBaseServiceCall()
							.getAuthenticateData(
									requestParamsDTO,
									selectResult = selectParamsService
									.getCQLCollectionData(requestParamsDTO
											.getFields(),selectValues)))) {
						
						resultSet = getCassandraService().getCollectionData(requestParamsDTO, processedData,collectionId,errorData,selectValues);
						if(includeTotal){
							Integer totalRows = resultSet.size();
							Map<String,String> sizeRecord = new HashMap<String, String>();
							sizeRecord.put("totalRows",""+totalRows);
							errorData.add(sizeRecord);
						}
						//						return getCollectionDAO().getCollectionData(requestParamsDTO, processedData.get("filterAggregate"),processedData.get("taxonomy"), processedData.get("tableType"),
//								processedData.get("startDateId"), processedData.get("endDateId"), processedData,collectionId,errorData);
						return defaultValues(selectResult.get("requestedValues"),resultSet);
						
					}
					
				}
		}
		}
		errorData.add(processedData);
		return null;
	}

	public List<Map<String, Object>> getCollectionResourceDetail(String data,
			String collectionId, String resourceId,List<Map<String,String>> errorData) throws ParseException {

		Map<String, String> processedData = new HashMap<String, String>();
		Map<String,String> selectValues = new HashMap<String, String>();
		
		if (getBaseServiceCall().checkData(data, errorData)) {
			RequestParamsDTO requestParamsDTO = this.getBaseServiceCall()
					.buildRequestParameters(data);

			if(requestParamsDTO.getFilters().getDataBase() != null ? requestParamsDTO.getFilters().getDataBase().equalsIgnoreCase("mysql") : false){
				
			if (getBaseServiceCall().validate(data, requestParamsDTO, errorData)) {
				if (getBaseServiceCall().canProceed(processedData = getBaseServiceCall()
						.getAuthenticateData(
								requestParamsDTO,
								selectParamsService
										.getCollectionResourceDetail(requestParamsDTO
												.getFields(),requestParamsDTO.getGroupBy())))) {

					return getCollectionDAO().getCollectionResourceDetail(
							requestParamsDTO,
							processedData.get("filterAggregate"),
							processedData.get("taxonomy"),
							processedData.get("tableType"),
							processedData.get("startDateId"),
							processedData.get("endDateId"), 
							processedData,
							collectionId, resourceId,errorData);

				}

			}
		}
		else{
			boolean includeTotal = false;
			if(requestParamsDTO.getPaginate() != null){
				includeTotal = true;
			}
			List<Map<String,Object>> resultSet = new ArrayList<Map<String,Object>>();
			Map<String,String> selectResult = new HashMap<String,String>();
			if (getBaseServiceCall().validate(data, requestParamsDTO, errorData)) {
				if (getBaseServiceCall().canProceed(processedData = getBaseServiceCall()
						.getAuthenticateData(
								requestParamsDTO,
								selectResult = selectParamsService
								.getCQLCollectionData(requestParamsDTO
										.getFields(),selectValues)))) {
					
					resultSet = getCassandraService().getCollectionData(requestParamsDTO, processedData,collectionId,errorData,selectValues);
					if(includeTotal){
						Integer totalRows = resultSet.size();
						Map<String,String> sizeRecord = new HashMap<String, String>();
						sizeRecord.put("totalRows",""+totalRows);
						errorData.add(sizeRecord);
					}
					return defaultValues(selectResult.get("requestedValues"),resultSet);
					//					return getCollectionDAO().getCollectionData(requestParamsDTO, processedData.get("filterAggregate"),processedData.get("taxonomy"), processedData.get("tableType"),
//							processedData.get("startDateId"), processedData.get("endDateId"), processedData,collectionId,errorData);
//					
					
				}
				
			}
		}
		}
		errorData.add(processedData);
		return new ArrayList<Map<String,Object>>();
	}

	public List<Map<String,Object>> defaultValues(String selectValues,List<Map<String,Object>> resultSet){
		 List<Map<String,Object>>  modifiedList = new ArrayList<Map<String,Object>>();
		for(Map<String,Object> value : resultSet){
			Map<String,Object> modifiedMap = new HashMap<String, Object>();
			for(String header : selectValues.split(",")){
				if(value.containsKey(header)){
					
					if(header.contains("status")){
						modifiedMap.put(header, (value.get("status").toString().equalsIgnoreCase("0")) ? "ACTIVE" : "DELETED");
					}else{
					modifiedMap.put(header, value.get(header));
					}
				}else{
					if(header.contains("timeSpent") || header.contains("views") || header.contains("averageGrade") || header.contains("questionCount") || header.contains("score") || header.contains("averageTimeSpent") || header.contains("correct") || header.contains("skip") || header.contains("wrong")){
						modifiedMap.put(header,new Integer(0) );
					}else if(header.contains("reaction")){
						modifiedMap.put(header,new String("NO Reaction"));
					}
					else{
						modifiedMap.put(header,new String("N/A") );
					}
				}
			}
			modifiedList.add(modifiedMap);
		}
		return modifiedList;
	}
	
	public void markDeletedResource(String startTime,String endTime){
	getCassandraService().markDeletedResource(startTime,endTime);	
	}
	
	public BaseService getBaseServiceCall(){
		
		return baseService;
	}
	
	public CollectionRestDAO getCollectionDAO(){
		
		return collectionRestDAO;
	}

	public CassandraService getCassandraService() {
		return cassandraService;
	}

	public void setCassandraService(CassandraService cassandraDAO) {
		this.cassandraService = cassandraDAO;
	}
	
	
	
	
}
