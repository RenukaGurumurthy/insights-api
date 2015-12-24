package org.gooru.insights.api.services;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.gooru.insights.api.models.EventObject;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.RequestParamsDTO;
import org.gooru.insights.api.utils.InsightsLogger;
import org.gooru.insights.api.utils.JsonDeserializer;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class ExcelReportServiceImpl implements ExcelReportService,InsightsConstant{

	@Autowired
	private MailerService mailerService;

	@Autowired
	private CSVBuilderService csvBuilderService;
	
	@Autowired
	private CassandraService cassandraService;

	@Autowired
	private BaseService baseService;
	
	private static ColumnList<String> exportEvents;
	
	private Gson gson = new Gson();

	private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	
	private static final SimpleDateFormat minFormatter = new SimpleDateFormat("yyyyMMddkkmm");
	
	private static final SimpleDateFormat utcDateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	@PostConstruct
	private void init(){
	cacheExportEvent();
	}
	
	private void cacheExportEvent(){
		if(exportEvents == null){
			exportEvents = cassandraService.getDashBoardKeys("export-events");
		}
	}
	
	private Map<String, String> getActivity(String data,String reportType) throws Exception {
		
		Map<String,String> result = new LinkedHashMap<String, String>();
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		List<Map<String, Object>> activityLogs = new ArrayList<Map<String,Object>>();
		
		String startTime = null;
		String endTime = null;
		String eventNames = null;
    	String eventParams = null;
		String supportedEvents = null;
		String supportedParams = null;
		Calendar cal = Calendar.getInstance();
    	
    	if((requestParamsDTO != null) && (requestParamsDTO.getFilters() != null)){ 
    		eventNames = requestParamsDTO.getFilters().getEventName();
    		eventParams = requestParamsDTO.getFilters().getEventParam();
    		
    		if((requestParamsDTO.getFilters().getStartDate() == null) || (requestParamsDTO.getFilters().getStartDate().isEmpty())){
    			throw new IllegalArgumentException("Start Date Can't be NULL");
    		}
    		if( (requestParamsDTO.getFilters().getEndDate() == null) || (requestParamsDTO.getFilters().getEndDate().isEmpty())){
    			throw new IllegalArgumentException("End Date Can't be NULL");
    		}
    		
    	}

    	if((requestParamsDTO != null) && (requestParamsDTO.getFilters() != null) && (requestParamsDTO.getFilters().getStartDate() != null) && (!requestParamsDTO.getFilters().getStartDate().isEmpty()) && (requestParamsDTO.getFilters().getEndDate() != null) && (!requestParamsDTO.getFilters().getEndDate().isEmpty())) {
    		startTime = requestParamsDTO.getFilters().getStartDate();
    		endTime = requestParamsDTO.getFilters().getEndDate();
    		
    	}
		
    	Map<String, Object> displayName = new Gson().fromJson(exportEvents.getColumnByName("exportDisplayNames").getStringValue(), new TypeToken<HashMap<String, Object>>() {}.getType());
		
    	supportedEvents = exportEvents.getColumnByName("eventNames").getStringValue();

    	supportedParams = exportEvents.getColumnByName("eventParams").getStringValue();
		
    	if(eventParams != null ){
	    	for(String paramss : eventParams.split(",")){
				if(!supportedParams.contains(paramss)){
					throw new IllegalArgumentException("Invalid parameter : "+ paramss);
				}
			}
		}else{
			eventParams = supportedParams;
		}
		if(eventNames != null){
	    	for(String eventss : eventNames.split(",")){
				if(!supportedEvents.contains(eventss)){
					throw new IllegalArgumentException("Invalid event name : "+ eventss);
				}
			}
		}else{
			eventNames = supportedEvents;
		}

    	List<String> timLinekeys = new ArrayList<String>();
				
			if(startTime.length() == 12 && endTime.length() == 12){
				for (Long startDate = Long.parseLong(startTime) ; startDate <= Long.parseLong(endTime);) {
					for(String event : eventNames.split(",")){
						timLinekeys.add(String.valueOf(startDate)+"~"+event);
					}
			    	try {
						cal.setTime(minFormatter.parse(String.valueOf(startDate)));
					} catch (ParseException e) {
						InsightsLogger.error(e);
					}
			    	cal.add(Calendar.MINUTE, 1);
			    	Date incrementedTime =cal.getTime(); 
			    	startDate = Long.parseLong(minFormatter.format(incrementedTime));
				}
				
			}else if(startTime.length() == 8 && endTime.length() == 8){
				for (Long startDate = Long.parseLong(startTime) ; startDate <= Long.parseLong(endTime);) {
					for(String event : eventNames.split(",")){
						timLinekeys.add(String.valueOf(startDate)+"~"+event);
					}
			    	try {
						cal.setTime(dateFormatter.parse(String.valueOf(startDate)));
					} catch (ParseException e) {
						InsightsLogger.error(e);
					}
			    	cal.add(Calendar.DATE, 1);
			    	Date incrementedTime =cal.getTime(); 
			    	startDate = Long.parseLong(dateFormatter.format(incrementedTime));
				}
				
			}else{
				throw new IllegalArgumentException("Invalid date format");
			}			
			String fileName = getDataFromCassandra(timLinekeys, eventNames, eventParams, requestParamsDTO, displayName, activityLogs);	 
			result.put("Download Link", "http:"+fileName);
			result.put("Message", "This download link will expire in 24 hours");		
		return result;
	}

	@Async
	private Map<String, String> getAsyncActivity(RequestParamsDTO requestParamsDTO, String reportType, String emailId) throws Exception {

		Map<String, String> result = new LinkedHashMap<String, String>();
		List<Map<String, Object>> activityLogs = new ArrayList<Map<String, Object>>();

		String startTime = null;
		String endTime = null;
		String eventNames = null;
		String supportedEvents = null;
		String supportedParams = null;
		String eventParams = null;
		Calendar cal = Calendar.getInstance();

		eventNames = requestParamsDTO.getFilters().getEventName();
		eventParams = requestParamsDTO.getFilters().getEventParam();
		startTime = requestParamsDTO.getFilters().getStartDate();
		endTime = requestParamsDTO.getFilters().getEndDate();

		Map<String, Object> displayName = new Gson().fromJson(exportEvents.getColumnByName("exportDisplayNames").getStringValue(), new TypeToken<HashMap<String, Object>>() {
		}.getType());

		supportedEvents = exportEvents.getColumnByName("eventNames").getStringValue();

		supportedParams = exportEvents.getColumnByName("eventParams").getStringValue();

		if (eventParams != null) {
			for (String paramss : eventParams.split(",")) {
				if (!supportedParams.contains(paramss)) {
					throw new IllegalArgumentException("Invalid parameter : " + paramss);
				}
			}
		} else {
			eventParams = supportedParams;
		}
		if (eventNames != null) {
			for (String eventss : eventNames.split(",")) {
				if (!supportedEvents.contains(eventss)) {
					throw new IllegalArgumentException("Invalid event name : " + eventss);
				}
			}
		} else {
			eventNames = supportedEvents;
		}

		List<String> timLinekeys = new ArrayList<String>();

		if (startTime.length() == 12 && endTime.length() == 12) {
			for (Long startDate = Long.parseLong(startTime); startDate <= Long.parseLong(endTime);) {
				for (String event : eventNames.split(",")) {
					timLinekeys.add(String.valueOf(startDate) + "~" + event);
				}
				try {
					cal.setTime(minFormatter.parse(String.valueOf(startDate)));
				} catch (ParseException e) {
					InsightsLogger.error(e);
				}
				cal.add(Calendar.MINUTE, 1);
				Date incrementedTime = cal.getTime();
				startDate = Long.parseLong(minFormatter.format(incrementedTime));
			}

		} else if (startTime.length() == 8 && endTime.length() == 8) {
			for (Long startDate = Long.parseLong(startTime); startDate <= Long.parseLong(endTime);) {
				for (String event : eventNames.split(",")) {
					timLinekeys.add(String.valueOf(startDate) + "~" + event);
				}
				try {
					cal.setTime(dateFormatter.parse(String.valueOf(startDate)));
				} catch (ParseException e) {
					InsightsLogger.error(e);
				}
				cal.add(Calendar.DATE, 1);
				Date incrementedTime = cal.getTime();
				startDate = Long.parseLong(dateFormatter.format(incrementedTime));
			}

		} else {
			throw new IllegalArgumentException("Invalid date format");
		}
		result.put("Message", "File download link will be sent to your email account");
		String fileName = getDataFromCassandra(timLinekeys, eventNames, eventParams, requestParamsDTO, displayName, activityLogs);
		getMailerService().sendMail(emailId, "HarvardX Data Dump - " + utcDateFormatter.format(new Date()), "Please download the attachement ", fileName);

		return result;
	}
	
	private int checkDateLimit(String startTime,String endTime){
		
		if(startTime.length() == 12 && endTime.length() == 12){
			Date endDate = null;
			try {
				endDate = minFormatter.parse(endTime);
			} catch (ParseException e) {
				InsightsLogger.error(e);
			}
			Date startDate = null;
			try {
				startDate = minFormatter.parse(startTime);
			} catch (ParseException e) {
				InsightsLogger.error(e);
			}
			int diffInDays = (int)( (endDate.getTime() - startDate.getTime() ) / (60 * 60 * 24 * 1000)) ;
			
			return diffInDays;
		}
		else if(startTime.length() == 8 && endTime.length() == 8){
		
			Date endDate = null;
			try {
				endDate = dateFormatter.parse(endTime);
			} catch (ParseException e) {
				InsightsLogger.error(e);
			}
			Date startDate = null;
			try {
				startDate = dateFormatter.parse(startTime);
			} catch (ParseException e) {
				InsightsLogger.error(e);
			}
			int diffInDays = (int)( (endDate.getTime() - startDate.getTime() ) / (60 * 60 * 24 * 1000));
			
			return diffInDays;
		}else{
			throw new IllegalArgumentException("Invalid date format");
		}
	}	
		
	private String  getDataFromCassandra(List<String> timLinekeys,String eventNames,String eventParams ,RequestParamsDTO requestParamsDTO,Map<String,Object> displayName,List<Map<String, Object>> activityLogs){
		int keysLimit = 100;
		String fileUrl = null;
		List<String> eventDetailkeys = new ArrayList<String>();	
		List<String> columns = new ArrayList<String>();
		columns.add("fields");
		for (int a = 0; a < timLinekeys.size(); a = a + keysLimit) {
			List<String> subTimeLineKeys = new ArrayList<String>();
			if((a + keysLimit) <= timLinekeys.size()){
				subTimeLineKeys = timLinekeys.subList(a, a + keysLimit);
			}else{
				subTimeLineKeys = timLinekeys.subList(a, timLinekeys.size());
			}
			OperationResult<Rows<String, String>> locationDetails = cassandraService.read(ColumnFamily.EVENT_TIMELINE.getColumnFamily(), subTimeLineKeys);
			 for (Row<String, String> row : locationDetails.getResult()) {
				 	ColumnList<String> eventUUID = row.getColumns();
				    if(eventUUID == null && eventUUID.isEmpty() ) {
				    	InsightsLogger.info("No events in given timeline");
				    }
			    	for(int i = 0 ; i < eventUUID.size() ; i++) {
			    		String eventDetailUUID = eventUUID.getColumnByIndex(i).getStringValue();
			    		eventDetailkeys.add(eventDetailUUID);
			    	}
			 }
			
		}
		for (int b = 0; b < eventDetailkeys.size(); b = b + keysLimit) {
				List<String> subEventDetailkeys = new ArrayList<String>();
				if((b + keysLimit) <= eventDetailkeys.size()){
					subEventDetailkeys = eventDetailkeys.subList(b, b + keysLimit);
				}else{
					subEventDetailkeys = eventDetailkeys.subList(b, eventDetailkeys.size());
				}
				OperationResult<Rows<String, String>> results = cassandraService.readAll(ColumnFamily.EVENT_DETAIL.getColumnFamily(), subEventDetailkeys, columns);
				 	for (Row<String, String> row : results.getResult()) {
				 		JsonObject eventObj = new JsonParser().parse(row.getColumns().getStringValue("fields", null)).getAsJsonObject();
				 		EventObject eventObject = gson.fromJson(eventObj, EventObject.class);
					 		try {
					 			Map<String,Object> allowedEvents = new LinkedHashMap<String, Object>();
								Map<String,Object> eventMap = JsonDeserializer.deserializeEventObject(eventObject);
								Date utcStartDate = new  Date(Long.valueOf(String.valueOf(eventMap.get("startTime"))));
								Date utcEndDate = new  Date(Long.valueOf(String.valueOf(eventMap.get("endTime"))));
								String sessionId = String.valueOf(eventMap.get("sessionId")).replaceAll("-", "");
								eventMap.put("startTime",utcDateFormatter.format(utcStartDate));
								eventMap.put("endTime",utcDateFormatter.format(utcEndDate));
								eventMap.put("sessionId",sessionId);
								if((requestParamsDTO != null) && (requestParamsDTO.getFilters() != null) && (requestParamsDTO.getFilters().getApiKey() != null) && (!requestParamsDTO.getFilters().getApiKey().isEmpty())){
									if(String.valueOf(eventMap.get("apiKey")).equals(requestParamsDTO.getFilters().getApiKey())){
										for(String params : eventParams.split(",")){
											if(eventMap.get(displayName.get(params)) != null){
												allowedEvents.put(params, eventMap.get(displayName.get(params)));
											}
										}
									}
								}else if((requestParamsDTO != null) && (requestParamsDTO.getFilters() != null) && (requestParamsDTO.getFilters().getOrganizationId() != null) && (!requestParamsDTO.getFilters().getOrganizationId().isEmpty())){
									if(String.valueOf(eventMap.get("organizationUId")).equals(requestParamsDTO.getFilters().getOrganizationId())){
										for(String params : eventParams.split(",")){
											if(eventMap.get(displayName.get(params)) != null){
												allowedEvents.put(params, eventMap.get(displayName.get(params)));
											}
										}
									}
								}else{						
										for(String params : eventParams.split(",")){
											if(eventMap.get(displayName.get(params)) != null){
												allowedEvents.put(params, eventMap.get(displayName.get(params)));
											}
										}
								}
							if(!allowedEvents.isEmpty()){
								activityLogs.add(allowedEvents);
							}
						} catch (JSONException e) {
							InsightsLogger.error(e);
					}
				 	}
				}
		 	try {
		 		 fileUrl = getCSVBuilderService().generateCSVMapReport(activityLogs,UUID.randomUUID()+"~"+minFormatter.format(new Date())+".csv");
			} catch (Exception e) {
				InsightsLogger.error(e);
			}
			return  fileUrl;
	}
	
	public Map<String, String> getPerformDump(String data,String format,String emailId) throws Exception{

		Map<String, String> finalData = new LinkedHashMap<String, String>();
		RequestParamsDTO requestParamsDTO = getBaseService().buildRequestParameters(data);
		getBaseService().existsFilter(requestParamsDTO);
			
    		if(!getBaseService().notNull(requestParamsDTO.getFilters().getStartDate())){
    			throw new IllegalArgumentException("Start Date Can't be NULL");
    		}
    		if(!getBaseService().notNull(requestParamsDTO.getFilters().getEndDate())){
    			throw new IllegalArgumentException("End Date Can't be NULL");
    		}
    		
		if(isAsyncProcess(requestParamsDTO.getFilters().getStartDate(), requestParamsDTO.getFilters().getEndDate())){
			if(!getBaseService().notNull(emailId)){
				finalData.put("Message", "Please provide atleast one email!!");
				return finalData;
    		}
			finalData  = getAsyncActivity(requestParamsDTO,format,emailId);
		}else{
			finalData  = getActivity(data,format);
		}
		return finalData;
	}
	
	public void removeExpiredFiles(){
		getCSVBuilderService().removeExpiredFiles();
	}
	
	private boolean isAsyncProcess(String startTime,String endTime){
		int requestedLimit = checkDateLimit(startTime, endTime);
		int exportLimit = exportEvents.getColumnByName("exportLimit").getIntegerValue();
		if(requestedLimit < exportLimit){
			return false;
		}
		return true;
	}
	
	private BaseService getBaseService() {
		return baseService;
	}

	private MailerService getMailerService() {
		return mailerService;
	}

	private CSVBuilderService getCSVBuilderService() {

		return csvBuilderService;
	}
}