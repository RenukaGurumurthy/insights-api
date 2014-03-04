/*******************************************************************************
 * SelectParamsServiceImpl.java
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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class SelectParamsServiceImpl implements SelectParamsService{

//mysql select values
	public Map<String, String> getCollectionData(String value,String groupBy) {
		boolean taxonomyGroupBy = false;
		if(groupBy != null && groupBy != "" && (!groupBy.isEmpty())){
			if(groupBy.contains("subject") || groupBy.contains("course") || groupBy.contains("unit") || groupBy.contains("topic") || groupBy.contains("lesson")){
			taxonomyGroupBy = true;
			}
		}
		Map<String, String> hibernateSelectValues = new HashMap<String, String>();
		hibernateSelectValues.put("timeSpent", "ROUND(SUM(CASE WHEN event_id IN (7,56) THEN agg.total_time_spent_ms ELSE '0' END)) AS timeSpent");
		hibernateSelectValues.put("views", "ROUND(SUM(CASE WHEN event_id IN (7,56) THEN agg.total_occurences ELSE 0 END )) AS views");
		hibernateSelectValues.put("averageTimeSpent", "ifnull(ROUND((SUM(CASE WHEN event_id IN (7,56) THEN agg.total_time_spent_ms END)/SUM(CASE WHEN event_id IN (7,56) THEN agg.total_occurences END))),0) AS averageTimeSpent");
		hibernateSelectValues.put("measure", "count(agg.resource_id) as measure");
		hibernateSelectValues.put("title", "IFNULL(r.title,'N/A') as title");
		hibernateSelectValues.put("thumbnail", "IFNULL(r.thumbnail,'N/A') as thumbnail");
		hibernateSelectValues.put("gooruOId", "IFNULL(agg.gooru_oid,'N/A') as gooruOId");
		hibernateSelectValues.put("reaction","CAST(CASE WHEN event_id =217 THEN CONCAT('{\"i-need-help\":\"',CASE WHEN event_value='i-need-help' THEN SUM(total_occurences) ELSE '0' END,'\",\"i-donot-understand\":\"',CASE WHEN event_value='i-donot-understand' THEN SUM(total_occurences) ELSE '0' END,'\",\"meh\":\"',CASE WHEN event_value='meh' THEN SUM(total_occurences) ELSE '0' END,'\",\"i-can-understand\":\"',CASE WHEN event_value='i-can-understand' THEN SUM(total_occurences) ELSE '0' END,'\",\"i-can-explain\":\"',CASE WHEN event_value='i-can-explain' THEN SUM(total_occurences) ELSE '0' END,'\",\"totalReaction\":','\"', SUM(total_occurences),'\"}' ) ELSE 'No Reaction' END AS CHAR(500)) AS reaction");
		hibernateSelectValues.put("reactionTimeSpent", "CAST(CASE WHEN event_id =217 THEN CONCAT('{\"i-need-help\":\"',CASE WHEN event_value='i-need-help' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"i-donot-understand\":\"',CASE WHEN event_value='i-donot-understand' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"meh\":\"',CASE WHEN event_value='meh' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"i-can-understand\":\"',CASE WHEN event_value='i-can-understand' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"i-can-explain\":\"',CASE WHEN event_value='i-can-explain' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"totalReaction\":','\"', SUM(total_time_spent_ms),'\"}' ) ELSE 'No Reaction' END  AS CHAR(500)) AS reactionTimeSpent");
		hibernateSelectValues.put("date","d.date AS date" );
		if(taxonomyGroupBy){
			hibernateSelectValues.put("subject", "subject");
			hibernateSelectValues.put("course", "course");
			hibernateSelectValues.put("unit", "unit");
			hibernateSelectValues.put("topic", "topic");
			hibernateSelectValues.put("lesson", "lesson");
		}else{
		hibernateSelectValues.put("subject", "GROUP_CONCAT(DISTINCT subject SEPARATOR '|') AS subject");
		hibernateSelectValues.put("course", "GROUP_CONCAT(DISTINCT course SEPARATOR '|') AS course");
		hibernateSelectValues.put("unit", "GROUP_CONCAT(DISTINCT unit SEPARATOR '|') AS unit");
		hibernateSelectValues.put("topic", "GROUP_CONCAT(DISTINCT topic SEPARATOR '|') AS topic");
		hibernateSelectValues.put("lesson", "GROUP_CONCAT(DISTINCT lesson SEPARATOR '|') AS lesson");
		}
		hibernateSelectValues.put("userUId", "agg.user_uid AS userUId");
		hibernateSelectValues.put("time", "time");
		hibernateSelectValues.put("likes", "SUM(agg.total_likes) AS likes");
		hibernateSelectValues.put("author", "u.username AS author");
		hibernateSelectValues.put("userName", "IFNULL(userAgg.userName,'N/A') AS userName");
		hibernateSelectValues.put("contentId", "IFNULL(r.content_id,'N/A') AS contentId");
		hibernateSelectValues.put("userViews", "IFNULL(collectionUserViews,0) AS userViews");
		hibernateSelectValues.put("userAverageTimeSpent", "IFNULL(ROUND(collectionUserAverageTimeSpent),0) AS userAverageTimeSpent");
		hibernateSelectValues.put("userReactionTimeSpent", "IFNULL(ROUND(collectionUserReactionTimeSpent),0) AS userReactionTimeSpent");
		hibernateSelectValues.put("userReaction", "IFNULL(collectionUserReaction,0) AS userReaction");
		hibernateSelectValues.put("userTimeSpent", "IFNULL(ROUND(collectionUserTimeSpent),0) AS userTimeSpent");
		hibernateSelectValues.put("description", "IFNULL(c.goals,'N/A') AS description");
		hibernateSelectValues.put("createdOn", "r.created_on AS createdOn");
		hibernateSelectValues.put("lastModified", "r.last_modified AS lastModified");
	
		Map<String, String> selectValues = getSelectValues(hibernateSelectValues, value);
	
		return selectValues;
	}
	
	
	public Map<String, String> getResourceData(String value,String groupBy) {
		boolean taxonomyGroupBy = false;
		if(groupBy != null && groupBy != "" && (!groupBy.isEmpty())){
			if(groupBy.contains("subject") || groupBy.contains("course") || groupBy.contains("unit") || groupBy.contains("topic") || groupBy.contains("lesson")){
			taxonomyGroupBy = true;
			}
		}
		Map<String, String> hibernateSelectValues = new HashMap<String, String>();
		hibernateSelectValues.put("timeSpent", "ROUND(SUM(CASE WHEN event_id IN (35,55) THEN agg.total_time_spent_ms ELSE '0' END)) AS timeSpent");
		hibernateSelectValues.put("views", "ROUND(SUM(CASE WHEN event_id IN (35,55) THEN agg.total_occurences ELSE 0 END )) AS views");
		hibernateSelectValues.put("averageTimeSpent", "ifnull(ROUND((SUM(CASE WHEN event_id IN (35,55) THEN agg.total_time_spent_ms END)/SUM(CASE WHEN event_id IN (35,55) THEN agg.total_occurences END))),0) AS averageTimeSpent");
		hibernateSelectValues.put("measure", "count(agg.resource_id) as measure");
		hibernateSelectValues.put("title", "IFNULL(r.title,'N/A') as title");
		hibernateSelectValues.put("thumbnail", "IFNULL(r.thumbnail,'N/A') as thumbnail");
		hibernateSelectValues.put("likes", "SUM(agg.total_likes) AS likes");
		hibernateSelectValues.put("gooruOId", "IFNULL(agg.gooru_oid,'N/A') as gooruOId");
		hibernateSelectValues.put("reaction","CAST(CASE WHEN event_id =217 THEN CONCAT('{\"i-need-help\":\"',CASE WHEN event_value='i-need-help' THEN SUM(total_occurences) ELSE '0' END,'\",\"i-donot-understand\":\"',CASE WHEN event_value='i-donot-understand' THEN SUM(total_occurences) ELSE '0' END,'\",\"meh\":\"',CASE WHEN event_value='meh' THEN SUM(total_occurences) ELSE '0' END,'\",\"i-can-understand\":\"',CASE WHEN event_value='i-can-understand' THEN SUM(total_occurences) ELSE '0' END,'\",\"i-can-explain\":\"',CASE WHEN event_value='i-can-explain' THEN SUM(total_occurences) ELSE '0' END,'\",\"totalReaction\":','\"', SUM(total_occurences),'\"}' ) ELSE 'No Reaction' END AS CHAR(500)) AS reaction");
		hibernateSelectValues.put("reactionTimeSpent", "CAST(CASE WHEN event_id =217 THEN CONCAT('{\"i-need-help\":\"',CASE WHEN event_value='i-need-help' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"i-donot-understand\":\"',CASE WHEN event_value='i-donot-understand' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"meh\":\"',CASE WHEN event_value='meh' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"i-can-understand\":\"',CASE WHEN event_value='i-can-understand' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"i-can-explain\":\"',CASE WHEN event_value='i-can-explain' THEN SUM(total_time_spent_ms) ELSE '0' END,'\",\"totalReaction\":','\"', SUM(total_time_spent_ms),'\"}' ) ELSE 'No Reaction' END  AS CHAR(500)) AS reactionTimeSpent");
		hibernateSelectValues.put("date","d.date AS date" );
		if(taxonomyGroupBy){
			hibernateSelectValues.put("subject", "subject");
			hibernateSelectValues.put("course", "course");
			hibernateSelectValues.put("unit", "unit");
			hibernateSelectValues.put("topic", "topic");
			hibernateSelectValues.put("lesson", "lesson");
		}else{
		hibernateSelectValues.put("subject", "GROUP_CONCAT(DISTINCT subject SEPARATOR '|') AS subject");
		hibernateSelectValues.put("course", "GROUP_CONCAT(DISTINCT course SEPARATOR '|') AS course");
		hibernateSelectValues.put("unit", "GROUP_CONCAT(DISTINCT unit SEPARATOR '|') AS unit");
		hibernateSelectValues.put("topic", "GROUP_CONCAT(DISTINCT topic SEPARATOR '|') AS topic");
		hibernateSelectValues.put("lesson", "GROUP_CONCAT(DISTINCT lesson SEPARATOR '|') AS lesson");
		}
		hibernateSelectValues.put("userUId", "agg.user_uid  AS userUId");
		hibernateSelectValues.put("time", "time");
		hibernateSelectValues.put("contentId", "IFNULL(agg.resource_id,'N/A') AS contentId");
		hibernateSelectValues.put("source", "IFNULL(r.attribution,'N/A') AS source");
		Map<String, String> selectValues = getSelectValues(hibernateSelectValues, value);
	
		return selectValues;
	}
	
	
	public Map<String, String> getCollectionResourceDetail(String value,String groupBy) {

		Map<String, String> hibernateSelectValues = new HashMap<String, String>();
		hibernateSelectValues.put("timeSpent", "IFNULL(ROUND(timeSpent),0) AS timeSpent");
		hibernateSelectValues.put("views", "IFNULL(ROUND(views),0) AS views");
		hibernateSelectValues.put("averageTimeSpent", "ifnull(ROUND(averageTimeSpent),0) AS averageTimeSpent");
		hibernateSelectValues.put("measure", "count(agg.resource_id) AS measure");
		hibernateSelectValues.put("title", "IFNULL(r.title,'N/A') AS title");
		hibernateSelectValues.put("thumbnail", "IFNULL(r.thumbnail,'N/A') AS thumbnail");
		hibernateSelectValues.put("resourceGooruOId", "r.gooru_oid AS resourceGooruOId");
		hibernateSelectValues.put("itemSequence", "IFNULL(item_sequence,0) AS itemSequence");
		hibernateSelectValues.put("collectionGooruOId", "IFNULL(agg.collectionGooruOId,0) AS collectionGooruOId");
		hibernateSelectValues.put("description", "IFNULL(r.description,'N/A') AS description");
		hibernateSelectValues.put("userUId", "IFNULL(agg.user_uid,'N/A') AS userUId");
		hibernateSelectValues.put("category", "IFNULL(r.category,'N/A') AS category");
		hibernateSelectValues.put("totalScore", "myScore AS totalScore");
		hibernateSelectValues.put("totalLike", "likes AS totalLike");
		hibernateSelectValues.put("source", "IFNULL(r.attribution,'N/A') AS source");
		hibernateSelectValues.put("status", "CASE WHEN ci.deleted =1 THEN 'DELETED' ELSE 'ACTIVE' END AS status");
		hibernateSelectValues.put("reaction", " IFNULL(agg.reaction,'NO Reaction') AS reaction");
		hibernateSelectValues.put("reactionTimeSpent", "reactionTimeSpent AS reactionTimeSpent");
		Map<String, String> selectValues = getSelectValues(hibernateSelectValues, value);
	
		return selectValues;
	}
	public Map<String,String> getSearchItem(String fields,String searchItem){
		Map<String, String> hibernateSelectValues = new HashMap<String, String>();
		if(searchItem.equalsIgnoreCase("all")){
			
			hibernateSelectValues.put("term", "agg.event_value as term");
			hibernateSelectValues.put("date","date");
			hibernateSelectValues.put("userUId", "agg.user_uid AS userUId");
			hibernateSelectValues.put("userName", "u.username AS userName");
			hibernateSelectValues.put("itemType", "'all' AS itemType");
			hibernateSelectValues.put("count", "SUM(agg.total_occurences) as count");
			
		}else if(searchItem.equalsIgnoreCase("collection")){
			hibernateSelectValues.put("term", "agg.event_value as term");
			hibernateSelectValues.put("date","date");
			hibernateSelectValues.put("userUId", "agg.user_uid AS userUId");
			hibernateSelectValues.put("userName", "u.username AS userName");
			hibernateSelectValues.put("itemType", "'collection' AS itemType");
			hibernateSelectValues.put("count", "SUM(agg.total_occurences) as count");
		}else if(searchItem.equalsIgnoreCase("resource")){
			hibernateSelectValues.put("term", "agg.event_value as term");
			hibernateSelectValues.put("date","date");
			hibernateSelectValues.put("userUId", "agg.user_uid AS userUId");
			hibernateSelectValues.put("userName", "u.username AS userName");
			hibernateSelectValues.put("count", "SUM(agg.total_occurences) as count");
			hibernateSelectValues.put("itemType", "'resource' AS itemType");
		}else{
			hibernateSelectValues.put("term", "agg.event_value as term");
			hibernateSelectValues.put("date","d.date");
			hibernateSelectValues.put("userUId", "agg.user_uid AS userUId");
			hibernateSelectValues.put("userName", "u.username AS userName");
			hibernateSelectValues.put("itemType", "'all' AS itemType");
			hibernateSelectValues.put("count", "SUM(agg.total_occurences) as count");
		}
		Map<String, String> selectValues = getSelectValues(hibernateSelectValues, fields);
		return selectValues;
	}
	
	//Excel Headers
	 public String[] getRegisterReport(){
		
		 String columnHeader[] ={"Times Clicked Signup","Registrations Completed","Regular Accounts","Gmail Accounts","SSO (Mileposts)",
				 "SSO (TFA)","Child Account","Parent Account","Other SSO Accounts"};
		 
		 return columnHeader;
	 }
	
	 public String[] getCollectionCreate(){
		 
		 String columnHeader[] ={"Collection ID", "Collection Title", "User ID (creator)", "Thumbanil (Y/N)", "Learning Objective", "Visibility", 
				 "Number of Views", "# of Resources in Collection", "# of Questions in Collection", "Date Last Edited (timestamp)", 
				 "Date Created (timestamp)", "Subject", "Course", "Grade", "Standards Alignment", "# Time Collection is Copied","#Thumbs Up"};
		 
		 return columnHeader;
	 }
	 
	 public String[] generateReport() {
		
		 String columnHeader[] = {"SCollection Title", "Scollection URL"," Views"," Average Time Spent(hh:mm:ss)"," Author"," Email ID"};
		 
		 return columnHeader;
	 }
	 public String[] getResourceCreated(){
			
		 String columnHeader[] ={"Resource ID", "Resource Title", "Resource Description", "Resource thumbnail (Y/N)", "User ID (creator)", 
				 "Resource Category (including question or file type)", "Length (if applicable)", "# of Views", 
				 "Date Created/Added", "# of Collections this is added", "Collection IDs of the C this Resource has been added" };
		 
		 return columnHeader;
	 }
	 
	public String[] getCollectionPlay(){
			
		 String columnHeader[] ={"collectionId","Gooru OID/ Resource Instance ID","Resource # in Collection","Title",
				 "Gooru  UID (who created the collection)","Course","Standards Alignment","Grade","Date Created","Date Last Modified","# of Views","# of Questions","Category Type of Resource (including Question type)","length of resource","averageTimeSpent","Scores","# Thumbs up","Status (Active/Deleted)"};

		 return columnHeader;
	 }
	 
	 public String[] getResourcePlay(){
			
		 String columnHeader[] ={"Resource ID","Resource Name","Resource Description","Thumbnail? (Y/N)","# of Views","Length of Resource (when applicable)",
				 "Resource Category (including question type)","Average Question Score",
				 "Number of Collections Included in (New/Aggregate)",
				 "Aggregate Reactions = (I Can Explain, I Understand, Meh, I Don't Understand, I Need Help)","Grades of Collection added in", "Subject", "Course", "Source"};
		
		 return columnHeader;
	 }
	 
	public String[] getClasspageReport() {

		String columnHeader[] = { "Classpage ID", "Classpage Title",
					"User ID (creator)", "Date Created", "Date last Edited",
					"Sharing", "Number of Assignments", "Image" };
		
		return columnHeader;
		}

	public String[] getAssignmentCreated() {

		String columnHeader[] = { "Assignment ID", "Assignment Title", "Due Date", "Description", "Classpage ID", "User ID (creator)", "# of Collections in Assignment",
				"Collection IDs of the C in this assignment", "Date Created", "Date Modified" };

		return columnHeader;
	}
	 //cassandra select values
	public Map<String,String> getCQLCollectionData(String value,Map<String,String> selectValues){
		if(!value.contains("resource")){
		selectValues.put("timeSpent", "TS~all");
		selectValues.put("views","VC~all");
		selectValues.put("questionCount", "total_qn_count");
		selectValues.put("score", "total_correct_ans");
		selectValues.put("gooruOId", "gooru_oid");
		}else{
			selectValues.put("resourceGooruOId", "gooru_oid");
			selectValues.put("timeSpent", "CP~TS~all");
			selectValues.put("views", "CP~VC~all");
			selectValues.put("collectionGooruOId", "parent_gooru_oid");
			}
		if(value.contains("reaction")){
			selectValues.put("reaction", "R~RA~2,R~RA~1,R~RA~3,R~RA~4,R~RA~5");
			selectValues.put("reactionTimeSpent", "R~RA~2~TS,R~RA~1~TS,R~RA~3~TS,R~RA~4~TS,R~RA~5~TS");
		}
		selectValues.put("averageGrade", "averageGrade");
		selectValues.put("title", "title");
		selectValues.put("description", "description");
		selectValues.put("lastModified", "last_modified");
		selectValues.put("category", "category");
		selectValues.put("thumbnail", "thumbnail");
		selectValues.put("correct", "CP~Q~all~correct");
		selectValues.put("wrong", "CP~Q~all~in-correct");
		selectValues.put("skip", "CP~Q~all~skipped");
		selectValues.put("status", "deleted");
		selectValues.put("itemSequence", "item_sequence");
		Map<String, String> hibernateSelectValues = getSelectValues(selectValues, value);
		return hibernateSelectValues;
		
	}
	
	//Get user preference vectors column names
	public Map<String,String> getUserPreferenceData(String value,Map<String,String> selectValues){		

		if(value.contains("category")){
			selectValues.put("category", "preferredCategory.exam,preferredCategory.handout,preferredCategory.interactive,preferredCategory.lesson,preferredCategory.slide,preferredCategory.textbook,preferredCategory.video,preferredCategory.website");
		} 
		if(value.contains("grade")){
			selectValues.put("grade", "preferredGrade.1,preferredGrade.2,preferredGrade.3,preferredGrade.4,preferredGrade.5,preferredGrade.6,preferredGrade.7,preferredGrade.8,preferredGrade.9,preferredGrade.10,preferredGrade.11,preferredGrade.12,preferredGrade.higher education,preferredGrade.kindergarten,preferredGrade.high school");
		}
		if(value.contains("subject")){
			selectValues.put("subject", "preferredSubject.math, preferredSubject.science, preferredSubject.technology & engineering, preferredSubject.social sciences, preferredSubject.language arts");
		}
		Map<String, String> hibernateSelectValues = getSelectValues(selectValues, value);
		return hibernateSelectValues;
		
	}
	
	public Map<String,String> getUserProficiencyData(String value,Map<String,String> selectValues){
		
		if(value.contains("subject")){
			selectValues.put("subject", "subject.");		
		}
		if(value.contains("course")){
			selectValues.put("course", "course.");
		}
		if(value.contains("unit")){
			selectValues.put("unit", "unit.");
		}
		if(value.contains("topic")){
			selectValues.put("topic", "topic.");
		}
		if(value.contains("lesson")){
			selectValues.put("lesson", "lesson.");
		}
		Map<String, String> hibernateSelectValues = getSelectValues(selectValues, value);
		return hibernateSelectValues;
	}
	
	//select values logics
	Map<String,String> getSelectValues(Map<String,String> hibernateSelectValues,String value){

		Map<String,String> requestedValues  = new HashMap<String, String>();
		StringBuffer selectValues = new StringBuffer();
		StringBuffer InvalidValues = new StringBuffer();
		StringBuffer requestList = new StringBuffer();
		int i = 0;
		Set<String> filterFields = avoidDuplicate(value);
		for (String selectValue: filterFields){
			
	    	  if(hibernateSelectValues.get(selectValue) != null){
	    		  i++;
	    		  
	    		  addComma(selectValues,hibernateSelectValues.get(selectValue));
	    		  addComma(requestList,selectValue);
	    		 
	    	  }else{
	    		  addErrorMessage(InvalidValues,selectValue);
	    	  }
	    	  
		}
		if(InvalidValues != null && InvalidValues.length() != 0){
		InvalidValues.append(")");
		}
		if(i == 0){
			InvalidValues.append("please use  values as specified in the document ");
		}
		requestedValues.put("select", selectValues.toString());
		requestedValues.put("InValidParameters", InvalidValues.toString());
		requestedValues.put("requestedValues", requestList.toString());
		requestedValues.put("totalFields", String.valueOf(i));
		return requestedValues;
	
}
	
	public void addComma(StringBuffer updatedCommaValues, String valueToAdd) {
		if (updatedCommaValues != null && updatedCommaValues.length() != 0) {
			updatedCommaValues.append(",");
		}
		updatedCommaValues.append(valueToAdd.trim());
	}
	
	
	public void addErrorMessage(StringBuffer InvalidValues, String valueToAdd) {
		if (InvalidValues != null && InvalidValues.length() != 0) {
			InvalidValues.append(",");
		}else{
			InvalidValues.append("Invalid parameters in fields (");
		}
		InvalidValues.append(valueToAdd.trim());
	}
	public Set<String> avoidDuplicate(String valueToAdd){
		
		Set<String> seter = new LinkedHashSet<String>();
		 try{
		        for(String data : valueToAdd.split(",")){
		        	seter.add(data);
		        }
		 }
		        catch(Exception e){}
		return seter;
	}
	
//	public String getRequestFields(String fieldData,List<Map<String,String>> errorData){
//
//		Map<String,String> errorDataMap = new HashMap<String, String>();
//		if(fieldData != null && fieldData !="" && (!fieldData.isEmpty())){
//			String numberFiltered = fieldData.replaceAll("\\d","");
//			 Set<String> dataSet = new TreeSet<String>();
//			 
//			 String breakInTOPices[] = numberFiltered.split("\\((.*?)\\)");
//			 String[] fieldKey = new String[breakInTOPices.length];
//			 for(int i=0;i<breakInTOPices.length;i++){
//					 fieldKey[i] =breakInTOPices[i];	 
//			 }
//			 if(fieldKey.length > 0){
//				 if(numberFiltered.contains("(")){
//			 Pattern pattern = Pattern.compile("\\((.*?)\\)");   
//			 Matcher matcher = pattern.matcher(numberFiltered);
//			 String value[] = new String[breakInTOPices.length];
//			 int j=0;
//			 while (matcher.find()) {
//				
//			  value[j] = matcher.group(1);
//			  j++;
//			 }
//			 boolean properFormat = false;
//			 String fieldSet="";
//			 int unwantedKey =-1;
//			 for(int k=0;k<j;k++){
//				 unwantedKey =k;
//				 properFormat = true;
//				 for(int l=k;l<fieldKey.length;l++){
//				 appendKey(value[k],dataSet,fieldKey[l]);
//				 break;
//				 }
//			 }
//			 if(unwantedKey != -1){
//				 unwantedKey = unwantedKey+1;
//				 for(int i=unwantedKey;i<fieldKey.length;i++){
//				 if(fieldSet !=""){
//					 fieldSet +=","+fieldKey[unwantedKey];
//				 }else{
//					 fieldSet +=fieldKey[unwantedKey];
//				 }
//				 }
//			 }
//			 if(fieldSet != ""){
//				 errorDataMap.put("additionalKeys", fieldSet);
//				 errorData.add(errorDataMap);
//			 }
//			 if(properFormat){
//				
//			 return getDataSet(dataSet);
//			 }else{
//				 errorDataMap.put("checkFieldsParameter", "No proper format for values in key.");
//			 }
//			 }else{
//					errorDataMap.put("checkFields", "There is no key name for the values.");
//				}
//
//		}
//			 	}else{
//		errorDataMap.put("CheckParameter", "Fields should not be empty provide key and value.key(value1,value2).");
//	}
//		errorData.add(errorDataMap);
//		return "";
//	}
//		 public static void appendKey(String commaValues,Set<String> dataSet,String keyValue){
//			 
//			 String data[] = commaValues.split(",");
//			for(int i=0;i<data.length;i++){
//				String values = data[i].replaceAll("[^a-zA-Z]+","");
//				if(!values.isEmpty() && values != null && values !=""){
//					dataSet.add(keyValue.replaceAll("[^a-zA-Z]+","")+""+values.substring(0,1).toUpperCase()+values.substring(1));
//				}
//			}
//			 
//		 }
		 
		 public String getDataSet(Set<String> data){
			 StringBuffer selectValues = new StringBuffer();
			 for(String field : data){
				 addComma(selectValues,field);
			 }
			 return selectValues.toString();
		 }
}
