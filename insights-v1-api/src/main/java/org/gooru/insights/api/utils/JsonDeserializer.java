/**
 * 
 */
package org.gooru.insights.api.utils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.gooru.insights.api.models.EventObject;
import org.gooru.insights.api.services.ExcelBuilderServiceImpl;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flexjson.JSONDeserializer;

/**
 * @author Insights Team
 * 
 */
public class JsonDeserializer {

	private static final Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);

	public static <T> T deserialize(String json, Class<T> clazz) {
		try {
			return new JSONDeserializer<T>().use(null, clazz).deserialize(json);
		} catch (Exception e) {
			logger.error("Exception in deserialize"+e);
			return null;
		}
	}
	
	public static <T> T deserializeTypeRef(String json, TypeReference<T> type) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, type);
		} catch (Exception e) {
			logger.error("Exception in deserialize"+e);
		}
		return null;
	}

	public static <T> T deserializeEventObject(EventObject eventObject) throws JSONException {
		
		ObjectMapper mapper = new ObjectMapper();
		Map<String,Object> map = new LinkedHashMap<String,Object>();
		try {
			map.put("eventName", eventObject.getEventName());
			map.put("endTime", eventObject.getEndTime());
			map.put("startTime", eventObject.getStartTime());
			map.put("eventId", eventObject.getEventId());
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getUser().toString(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getMetrics().toString(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getPayLoadObject().toString(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getContext().toString(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getSession().toString(), new TypeReference<HashMap<String,String>>(){}));
		} catch (Exception e) {
			logger.error("Exception in deserialize"+e);
		}
		return (T) map;
	}
	
}
