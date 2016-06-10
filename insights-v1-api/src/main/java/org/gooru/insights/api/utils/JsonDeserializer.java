/**
 *
 */
package org.gooru.insights.api.utils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flexjson.JSONDeserializer;

/**
 * @author Insights Team
 *
 */
public final class JsonDeserializer {

	private static final Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);

	private JsonDeserializer() {
		throw new AssertionError();
	}

	public static <T> T deserialize(String json, Class<T> clazz) {
		try {
			return new JSONDeserializer<T>().use(null, clazz).deserialize(json);
		} catch (Exception e) {
			logger.error("Exception in deserialize", e);
			return null;
		}
	}

	public static <T> T deserializeTypeRef(String json, TypeReference<T> type) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, type);
		} catch (Exception e) {
			logger.error("Exception in deserialize", e);
		}
		return null;
	}
}
