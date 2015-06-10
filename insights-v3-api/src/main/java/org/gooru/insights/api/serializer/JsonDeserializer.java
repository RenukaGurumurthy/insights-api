/**
 * 
 */
package org.gooru.insights.api.serializer;


import org.gooru.insights.api.exception.BadRequestException;

import com.fasterxml.jackson.core.type.TypeReference;

import flexjson.JSONDeserializer;



/**
 * @author Insights Team
 * 
 */
public class JsonDeserializer extends JsonProcessor {

	public static <T> T deserialize(String json, Class<T> clazz) {
		try {
			return new JSONDeserializer<T>().use(null, clazz).deserialize(json);
		} catch (Exception e) {
			throw new BadRequestException("Input JSON parse failed!");
		}
	}
	
	public static <T> T deserialize(String json, TypeReference<T> type) {
		try {
			return getMapper().readValue(json, type);
		} catch (Exception e) {
			throw new BadRequestException("Input JSON parse failed! " +  e);
		}
	}
}
