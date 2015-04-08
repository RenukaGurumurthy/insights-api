package org.gooru.insights.api.services;

import java.util.Map;

public interface SelectParamsService {
	
	 String getClasspageDetail(String value,Map<String,String> hibernateSelectValues) throws Exception;
	 
	 String getClassExportDetail(String value,Map<String,String> hibernateSelectValues) throws Exception;
	 
	 String getAggCollectionExportDetail(String value,Map<String,String> hibernateSelectValues) throws Exception;
	 
	 String getClassResourceExportDetail(String value,Map<String,String> hibernateSelectValues) throws Exception;
	 
	 String getClasspageCollectionDetail(String value,Map<String,String> hibernateSelectValues) throws Exception;
	 
	 Map<String,String> getLiveDashboardData(String fields) throws Exception;
	 
	 String getUserPreferenceData(String value,Map<String,String> selectValues) throws Exception;
	 
	 String getUserProficiencyData(String value,Map<String,String> selectValues) throws Exception;
	 
	 String getClasspageCollectionUsage(String value,Map<String,String> selectValues) throws Exception;
	 
	 String getClasspageResourceUsage(String value,Map<String,String> selectValues) throws Exception;
	 
	 String getClasspageUser(String value,Map<String,String> selectValues) throws Exception;
	 
	 String getUser(String value,Map<String,String> selectValues) throws Exception;
	 
	 String getOEResource(String fields,Map<String,String> selectValues) throws Exception;
	 
}
