/*******************************************************************************
 * CollectionRestDAOImpl.java
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.insights.api.models.RequestParamsDTO;
import org.gooru.insights.api.models.ResponseParamsDTO;
import org.gooru.insights.api.services.CassandraService;
import org.gooru.insights.api.services.SelectParamsService;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class CollectionRestDAOImpl implements CollectionRestDAO{

	@Autowired
	private BaseDAO baseDAO;
	
	private ResponseParamsDTO responseParamsDTO;
	
	@Autowired
	private CassandraService cassandraService;
	 
	@Autowired
	private SessionFactory sessionFactory;
	
	@Autowired
	private SelectParamsService selectParamsService;
	
	 public List<Map<String, Object>> getCollectionData(RequestParamsDTO requestParamsDTO, String filterAggregate, String taxonomy, String tableType, String startDateId, String endDateId,
				Map<String, String> hibernateSelectValues,String collectionId,List<Map<String,String>> errorData) {

		
			String selectFields = hibernateSelectValues.get("select");
			String InvalidFields = hibernateSelectValues.get("InValidParameters");
			String requestFields = hibernateSelectValues.get("requestedValues");
			String noOfFields = hibernateSelectValues.get("totalFields");
			String unixStartDate = hibernateSelectValues.get("unixStartDate");
			String unixEndDate = hibernateSelectValues.get("unixEndDate");
			Integer totalFields = 0;
			boolean firstOnCondition = false;
			boolean resourceTableInvoked = false;
			StringBuffer ANDSb = new StringBuffer();
			StringBuffer dataBuff = new StringBuffer();
			
			Map<String, Map<String, Integer>> paramList = new HashMap<String, Map<String, Integer>>();
			
			if (getBaseDAO().checkNull(selectFields.toString())) {
				totalFields = Integer.parseInt(noOfFields.toString());
			}

			String sql = " FROM agg_event_resource_"+tableType+"_"+filterAggregate+" as agg ";
			if(requestFields.contains("createdOn") || requestFields.contains("lastModified") || requestFields.contains("author") || requestFields.contains("thumbnail") || requestFields.contains("title")){
				resourceTableInvoked = true;
				sql +=" INNER JOIN" + " dim_resource as r on (agg.resource_id=r.content_id  ";
				if(!firstOnCondition){
				sql += "AND"+this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList);
				firstOnCondition = true;
				}
				sql +=")";
				}
			if(requestFields.contains("description")){
				sql +=" INNER JOIN collection AS c ON (c.content_id = r.content_id ";
				if(!firstOnCondition){
					sql +="AND"+ this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList);
					firstOnCondition = true;
					}
					sql +=")";
					}
			if(requestFields.contains("author")){
				
				sql +=" INNER JOIN user AS u ON (u.gooru_uid = r.user_uid ";
				if(!firstOnCondition){
					sql +="AND"+ this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList);
					firstOnCondition = true;
					}
					sql +=")";
					}


			if(getBaseDAO().checkNull(requestParamsDTO.getFilters().getPartnerId())){
				if(requestParamsDTO.getFilters().getPartnerId().equalsIgnoreCase("2")){
					if(!resourceTableInvoked){
					sql +=" INNER JOIN" + " dim_resource as r on (agg.resource_id=r.content_id ";
							if(!firstOnCondition){
								sql +="AND"+ this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList);
								firstOnCondition = true;
								}
								sql +=")";
								}
					sql +=" INNER JOIN identity AS i on (r.user_uid = i.user_uid AND i.external_id like  '%Autodesk%' )";	
				}else if(requestParamsDTO.getFilters().getPartnerId().equalsIgnoreCase("3")){
					if(!resourceTableInvoked){
						sql +=" INNER JOIN" + " dim_resource as r on (agg.resource_id=r.content_id ";
								if(!firstOnCondition){
									sql +="AND"+ this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList);
									firstOnCondition = true;
									}
									sql +=")";
									}
					sql +=" INNER JOIN identity AS i on (r.user_uid = i.user_uid AND i.external_id like '%rusd%' )";	
				}else if(requestParamsDTO.getFilters().getPartnerId().equalsIgnoreCase("5")){
					sql +=" INNER JOIN identity AS i on (agg.user_uid = i.user_uid AND i.external_id like '%rusd%'  ";
					if(!firstOnCondition){
						sql +="AND"+ this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList);
						firstOnCondition = true;
						}
						sql +=")";
				}else if(requestParamsDTO.getFilters().getPartnerId().equalsIgnoreCase("6")){
				sql +=" INNER JOIN identity AS i on (agg.user_uid = i.user_uid AND  i.idp_id=1278   ";
				if(!firstOnCondition){
					sql +="AND"+ this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList);
					firstOnCondition = true;
					}
					sql +=")";
				}
			}
			if(requestFields.contains("subject") || requestFields.contains("course") || requestFields.contains("unit") || requestFields.contains("topic") || requestFields.contains("lesson")){
				sql +=" INNER JOIN dim_content_classification AS dc ON (dc.content_id = agg.resource_id " ;
				if(!firstOnCondition){
					sql +="AND"+ this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList);
					firstOnCondition = true;
					}
					sql +=")";
					sql +=	" INNER JOIN fact_taxonomy AS ft ON (ft.lesson_code_id = dc.code_id ";
				if (getBaseDAO().checkNull(taxonomy)) {

					sql +=" AND " + taxonomy + "";
				}
				sql +=")";
			}
			
					if(requestFields.contains("userTimeSpent") || requestFields.contains("userViews") || requestFields.contains("userAverageTimeSpent") || requestFields.contains("userReactionTimeSpent") || requestFields.contains("userReaction") || requestFields.contains("userName")){
						
						ANDSb = new StringBuffer();
						sql +=" LEFT JOIN (SELECT agg.resource_id AS resource_id2,SUM(CASE WHEN event_id IN (7,56) THEN agg.total_occurences ELSE 0 END ) AS collectionUserViews,SUM(CASE WHEN event_id IN (7,56) THEN agg.total_time_spent_ms END) AS collectionUserTimeSpent," +
								"(SUM(CASE WHEN event_id IN (7,56) THEN agg.total_time_spent_ms END)/SUM(CASE WHEN event_id IN (7,56) THEN agg.total_occurences END)) AS collectionUserAverageTimeSpent," +
								"SUM(CASE WHEN event_id=217 THEN total_time_spent_ms ELSE 0 END) AS collectionUserReactionTimeSpent,SUM(CASE WHEN event_id=217 THEN total_occurences ELSE 0 END) AS collectionUserReaction,u.username AS userName " +
								"FROM agg_event_resource_"+tableType+"_"+filterAggregate+" as agg   INNER JOIN user AS u on (agg.user_uid = u.gooru_uid) " ;
					
						if(requestFields.contains("subject") || requestFields.contains("course") || requestFields.contains("unit") || requestFields.contains("topic") || requestFields.contains("lesson")){
							sql +=" INNER JOIN dim_content_classification AS dc ON (dc.content_id = agg.resource_id )" ;
								sql +=	" INNER JOIN fact_taxonomy AS ft ON (ft.lesson_code_id = dc.code_id ";
							if (getBaseDAO().checkNull(taxonomy)) {

								sql +=" AND " + taxonomy + "";
							}
							sql +=")";
						}
						getBaseDAO().addWhereClause(ANDSb, " agg.event_id in (7,56,217) ");
						if (getBaseDAO().checkNull(startDateId) && getBaseDAO().checkNull(endDateId)) {
							getBaseDAO().addWhereClause(ANDSb, " agg.date_id between '" + startDateId + "' AND '" + endDateId + "' ");

						} else if (getBaseDAO().checkNull(startDateId)) {
							getBaseDAO().addWhereClause(ANDSb, " agg.date_id ='" + startDateId + "' ");

						}
						if (getBaseDAO().checkNull(requestParamsDTO.getFilters().getUserUId())) {
							
						getBaseDAO().addWhereClause(ANDSb, " agg.user_uid='" + requestParamsDTO.getFilters().getUserUId() + "' ");

						}
						
						if (ANDSb.length() > 0) {
							sql += " where " + ANDSb.toString();
						}
						sql +=" group by agg.resource_id) AS userAgg ON (userAgg.resource_id2 = agg.resource_id) ";
					}
					
					if(requestFields.contains("date")){
						requestFields +=",unixDate";
						selectFields +=",UNIX_TIMESTAMP(date) AS unixDate";
						totalFields +=1;
						if (filterAggregate.equalsIgnoreCase("hour")) {
							if(requestFields.contains("time")){
								selectFields +=",agg.time_id AS time";
								totalFields +=1;
							}
						}
							sql +=" INNER JOIN dim_date AS d ON (d.date_id=agg.date_id ";
							if(!firstOnCondition){
								sql +="AND"+ this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList);
								firstOnCondition = true;
								}
							sql +=")";
							
					if (getBaseDAO().checkNull(startDateId) && getBaseDAO().checkNull(endDateId)) {
						getBaseDAO().addWhereClause(dataBuff," d.date_id between '" + startDateId + "' AND '" + endDateId + "' ");

					} else if (getBaseDAO().checkNull(startDateId)) {
						getBaseDAO().addWhereClause(dataBuff," d.date_id ='" + startDateId + "'");

					}
					if (filterAggregate.equalsIgnoreCase("month")) {
						getBaseDAO().addWhereClause(dataBuff, "d.day = 1  ");

					} else if (filterAggregate.equalsIgnoreCase("year")) {
						getBaseDAO().addWhereClause(dataBuff, "d.day = 1  AND d.month = 1");

					}
					
					
				}
					if(!firstOnCondition){
						getBaseDAO().addWhereClause(dataBuff,this.collectionFirstON(requestParamsDTO, startDateId, endDateId, collectionId, requestFields, paramList));
						firstOnCondition = true;
						}
					if (dataBuff.length() > 0) {
						sql += " WHERE " + dataBuff.toString();
					}
				String groupBy= " group by resource_id ";

			return getBaseDAO().processRecords(requestParamsDTO, sql, selectFields, InvalidFields, requestFields, totalFields,unixStartDate,unixEndDate,groupBy,paramList,errorData);
			}
	
	 public List<Map<String, Object>> getCollectionResourceDetail(RequestParamsDTO requestParamsDTO, String filterAggregate, String taxonomy, String tableType, String startDateId, String endDateId,
				Map<String, String> hibernateSelectValues,String collectionId,String resourceId,List<Map<String,String>> errorData){
		 

			String selectFields = hibernateSelectValues.get("select");
			String InvalidFields = hibernateSelectValues.get("InValidParameters");
			String requestFields = hibernateSelectValues.get("requestedValues");
			String noOfFields = hibernateSelectValues.get("totalFields");
			String unixStartDate = hibernateSelectValues.get("unixStartDate");
			String unixEndDate = hibernateSelectValues.get("unixEndDate");
			Integer totalFields = 0;
			Integer checkCollectionFlag = 0;
			Integer checkResourceFlag = 0;
			String collectionContentId =null;
			Map<String, Map<String, Integer>> paramList = new HashMap<String, Map<String, Integer>>();
			
			if (getBaseDAO().checkNull(noOfFields)) {
				totalFields = Integer.parseInt(noOfFields);
			}

			String sql = " FROM  collection_item as ci  LEFT JOIN (SELECT resource_id,SUM(CASE WHEN event_id in (181,10) THEN total_occurences END) AS views,CAST(CASE WHEN event_id =217 THEN CONCAT('[{\"i-need-help\":\"',SUM(CASE WHEN event_value='i-need-help' THEN total_occurences ELSE '0' END),'\",\"i-donot-understand\":\"',SUM(CASE WHEN event_value='i-donot-understand' THEN total_occurences ELSE '0' END),'\",\"meh\":\"',SUM(CASE WHEN event_value='meh' THEN total_occurences ELSE '0' END),'\",\"i-can-understand\":\"',SUM(CASE WHEN event_value='i-can-understand' THEN total_occurences ELSE '0' END),'\",\"i-can-explain\":\"',SUM(CASE WHEN event_value='i-can-explain' THEN total_occurences ELSE '0' END),'\",\"totalReaction\":','\"', SUM(total_occurences),'\"}]' ) ELSE 'NO Reaction' END AS CHAR(500)) AS reaction , " +
					" SUM(CASE WHEN event_id IN (181,10) THEN total_time_spent_ms END) AS timespent, " +
					" CASE WHEN event_id IN (181,10) THEN IFNULL(sum(total_time_spent_ms)/sum(total_occurences),0) END AS averageTimeSpent, " +
					"CAST(CASE WHEN event_id =217 THEN CONCAT('[{\"i-need-help\":\"',SUM(CASE WHEN event_value='i-need-help' THEN total_time_spent_ms ELSE '0' END),'\",\"i-donot-understand\":\"',SUM(CASE WHEN event_value='i-donot-understand' THEN total_time_spent_ms ELSE '0' END),'\",\"meh\":\"',SUM(CASE WHEN event_value='meh' THEN total_time_spent_ms ELSE '0' END),'\",\"i-can-understand\":\"',SUM(CASE WHEN event_value='i-can-understand' THEN total_time_spent_ms ELSE '0' END),'\",\"i-can-explain\":\"',SUM(CASE WHEN event_value='i-can-explain' THEN total_time_spent_ms ELSE '0' END),'\",\"totalReaction\":','\"', SUM(total_time_spent_ms),'\"}]' ) ELSE 'NO Reaction' END AS CHAR(500)) AS reactionTimeSpent, " +
					" SUM(CASE WHEN event_id IN (181,10) THEN total_likes END)   AS likes, " +
					" SUM(CASE WHEN event_id IN (181,10) THEN total_score ELSE 0 END) AS myScore," +
					" agg.user_uid,agg.parent_gooru_oid as collectionGooruOId  FROM agg_event_resource_"+tableType+"_"+filterAggregate+" as agg ";
		 
				
				if(requestFields.contains("date")){
					requestFields +=",unixDate";
					selectFields +=",UNIX_TIMESTAMP(date) AS unixDate";
					totalFields +=1;
					sql +=" INNER JOIN dim_date AS d ON (d.date_id=agg.date_id) ";
				}
				
				if(requestFields.contains("subject") || requestFields.contains("course") || requestFields.contains("unit") || requestFields.contains("topic") || requestFields.contains("lesson")){
					
					sql +=" INNER JOIN dim_content_classification AS dc ON (dc.content_id = agg.resource_id) INNER JOIN fact_taxonomy AS ft ON (ft.lesson_code_id = dc.code_id) ";
				
				}
			StringBuffer whereClauseSb = new StringBuffer();

			getBaseDAO().addWhereClause(whereClauseSb, " agg.event_id in (181,10,217) ");

			if(getBaseDAO().checkNull(collectionId)){
				checkCollectionFlag = 1;
					Map<String, Integer> paramMap = new HashMap<String, Integer>();
					paramMap.put(collectionId, checkCollectionFlag);
					Session session = sessionFactory.getCurrentSession();
					Query sql2 = session.createSQLQuery("SELECT content_id FROM dim_resource where gooru_oid in (:gooruOId)");
					sql2.setParameterList("gooruOId", collectionId.split(","));
					List<Object> id = sql2.list();
				for(Object data : id){
					if(collectionContentId != null){
						collectionContentId +=","+data;
					}else{
					collectionContentId =data.toString();
					}
				}
					paramList.put("collectionId", paramMap);
				getBaseDAO().addWhereClause(whereClauseSb, " agg.parent_gooru_oid IN  (:collectionId)  ");
			}else if(getBaseDAO().checkNull(requestParamsDTO.getFilters().getCollectionGooruOId())){
				getBaseDAO().addWhereClause(whereClauseSb, " agg.parent_gooru_oid = '"+requestParamsDTO.getFilters().getCollectionGooruOId()+"' ");
			}
			
			if(getBaseDAO().checkNull(requestParamsDTO.getFilters().getUserUId())){
				getBaseDAO().addWhereClause(whereClauseSb, " agg.user_uid = '"+requestParamsDTO.getFilters().getUserUId()+"' ");
			}
			
			if(getBaseDAO().checkNull(resourceId)){
				checkResourceFlag = 1;
				Map<String, Integer> paramMap = new HashMap<String, Integer>();
				paramMap.put(resourceId, checkResourceFlag);
				paramList.put("resourceId", paramMap);
			getBaseDAO().addWhereClause(whereClauseSb, " agg.gooru_oid IN  (:resourceId)  ");
			}else if(getBaseDAO().checkNull(requestParamsDTO.getFilters().getResourceGooruOId())){
				getBaseDAO().addWhereClause(whereClauseSb, " agg.gooru_oid = '"+requestParamsDTO.getFilters().getResourceGooruOId()+"' ");
			}
			
			if (getBaseDAO().checkNull(startDateId) && getBaseDAO().checkNull(endDateId)) {
				getBaseDAO().addWhereClause(whereClauseSb, " agg.date_id between '" + startDateId + "' AND '" + endDateId + "' ");

			} else if (getBaseDAO().checkNull(startDateId)) {
				getBaseDAO().addWhereClause(whereClauseSb, " agg.date_id ='" + startDateId + "' ");

			}
			
			if (whereClauseSb.length() > 0) {
				sql += " WHERE " + whereClauseSb.toString();
			}
			
			sql += " group by resource_id ";
			
					sql +=") as agg ON (ci.resource_content_id=agg.resource_id) " +
					"left join dim_resource as r on (ci.resource_content_id=r.content_id) ";
					whereClauseSb = new StringBuffer();
					if(getBaseDAO().checkNull(collectionContentId)){
						getBaseDAO().addWhereClause(whereClauseSb, " ci.collection_content_id in (" + collectionContentId + ") ");

					}
			
			if(checkCollectionFlag == 0){
			if (getBaseDAO().checkNull(requestParamsDTO.getFilters().getCollectionContentId())) {
				getBaseDAO().addWhereClause(whereClauseSb, " ci.collection_content_id =" + requestParamsDTO.getFilters().getCollectionContentId() + " ");

			}
			}
			if (getBaseDAO().checkNull(requestParamsDTO.getFilters().getAuthorUId())) {
				getBaseDAO().addWhereClause(whereClauseSb, " r.creator_uid ='" + requestParamsDTO.getFilters().getAuthorUId() + "' ");

			}
			
			if (getBaseDAO().checkNull(requestParamsDTO.getFilters().getResourceCategory())) {
				getBaseDAO().addWhereClause(whereClauseSb, " r.category ='" + requestParamsDTO.getFilters().getResourceCategory() + "' ");

			}
			
			if (getBaseDAO().checkNull(requestParamsDTO.getFilters().getResourceSourceLabel())) {
				getBaseDAO().addWhereClause(whereClauseSb, " r.attribution ='" + requestParamsDTO.getFilters().getResourceSourceLabel() + "' ");

			}
			
			if (getBaseDAO().checkNull(requestParamsDTO.getFilters().getDeletedResource())) {
				getBaseDAO().addWhereClause(whereClauseSb, " ci.deleted ='" + requestParamsDTO.getFilters().getDeletedResource() + "' ");

			}
			
			if (whereClauseSb.length() > 0) {
				sql += " WHERE " + whereClauseSb.toString();
			}
			
			String groupBy= " ";
			
		 return getBaseDAO().processRecords(requestParamsDTO, sql, selectFields, InvalidFields, requestFields, totalFields,unixStartDate,unixEndDate,groupBy,paramList,errorData);
		 
	 }
	 
		public String collectionFirstON(RequestParamsDTO requestParamsDTO,String startDateId,String endDateId,String collectionId,String requestFields,Map<String,Map<String, Integer>> paramList){
			String onCondition ="";
			StringBuffer condition = new StringBuffer();
			Integer checkFlag = 0;
			getBaseDAO().addWhereClause(condition, " agg.event_id in (7,56,217) ");

			if(getBaseDAO().checkNull(collectionId)){
				checkFlag = 1;
				Map<String, Integer> paramMap = new HashMap<String, Integer>();
				paramMap.put(collectionId, checkFlag);
				paramList.put("collectionId", paramMap);
				getBaseDAO().addWhereClause(condition, " agg.gooru_oid IN  (:collectionId) ");
			}
				if (getBaseDAO().checkNull(startDateId) && getBaseDAO().checkNull(endDateId)) {
					getBaseDAO().addWhereClause(condition, " agg.date_id between '" + startDateId + "' AND '" + endDateId + "' ");

				} else if (getBaseDAO().checkNull(startDateId)) {
					getBaseDAO().addWhereClause(condition, " agg.date_id ='" + startDateId + "' ");

				}
				if((!requestFields.contains("userTimeSpent") && !requestFields.contains("userViews") && !requestFields.contains("userAverageTimeSpent") && !requestFields.contains("userReactionTimeSpent") && !requestFields.contains("userReaction"))){
				if (getBaseDAO().checkNull(requestParamsDTO.getFilters().getUserUId())) {
					
					getBaseDAO().addWhereClause(condition, " agg.user_uid='" + requestParamsDTO.getFilters().getUserUId() + "' ");

					}
				}
					
			if(condition.length() > 0){
				onCondition =condition.toString();
			}
			return onCondition;
		}
	
	public BaseDAO getBaseDAO(){
		
		return baseDAO;
	}
}
