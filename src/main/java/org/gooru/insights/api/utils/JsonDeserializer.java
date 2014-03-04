/**
 * 
 */
package org.gooru.insights.api.utils;

import flexjson.JSONDeserializer;

/**
 * @author Search Team
 * 
 */
public class JsonDeserializer extends JsonProcessor {

	public static <T> T deserialize(String json, Class<T> clazz) {
		try {
			return new JSONDeserializer<T>().use(null, clazz).deserialize(json);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
