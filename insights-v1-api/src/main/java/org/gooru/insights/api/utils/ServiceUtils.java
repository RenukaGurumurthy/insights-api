package org.gooru.insights.api.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public final class ServiceUtils {

	private static final Gson gson = new Gson();

	private ServiceUtils() {
		throw new AssertionError();
	}

	public static String appendComma(String... texts) {
		StringBuilder sb = new StringBuilder();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ApiConstants.COMMA);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}

	public static String appendTilda(String... texts) {
		StringBuilder sb = new StringBuilder();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ApiConstants.TILDA);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}

	public static String appendHyphen(String... texts) {
		StringBuilder sb = new StringBuilder();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ApiConstants.HYPHEN);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}

	public static Object castJSONToList(String data) {
		if(StringUtils.isBlank(data) || ApiConstants.NA.equals(data)) {
			return new ArrayList<Map<String, Object>>();
		}
		try {
			return gson.fromJson(data, (new TypeToken<List<Map<String, Object>>>() {}).getType());
		}catch(JsonSyntaxException e) {
			return new ArrayList<Map<String, Object>>();
		}
	}

	public static List<Map<String, Object>> sortBy(List<Map<String, Object>> requestData, String sortBy, String sortOrder) {

		if (StringUtils.isNotBlank(sortBy)) {
			for (final String name : sortBy.split(ApiConstants.COMMA)) {
				boolean descending = false;
				if (StringUtils.isNotBlank(sortOrder) && sortOrder.equalsIgnoreCase(ApiConstants.DESC)) {
					descending = true;
				}
				if (descending) {
					Collections.sort(requestData, (m1, m2) -> {
                        if (m2.containsKey(name)) {
                            if (m1.containsKey(name)) {
                                return compareTo(m2, m1, name);
                            } else {
                                return 1;
                            }
                        } else {
                            return -1;
                        }
                    });

				} else {
					Collections.sort(requestData, (m1, m2) -> {
                        if (m1.containsKey(name) && m2.containsKey(name)) {
                            return compareTo(m1, m2, name);
                        }
                        return 1;
                    });
				}
			}
		}
		return requestData;
	}

	private static int compareTo(Map<String, Object> m1, Map<String, Object> m2, String name) {
		if (m1.get(name) instanceof String) {
			return m1.get(name).toString().toLowerCase().compareTo(m2.get(name).toString().toLowerCase());
		} else if (m1.get(name) instanceof Long) {
			return ((Long) m1.get(name)).compareTo((Long) m2.get(name));
		} else if (m1.get(name) instanceof Integer) {
			return ((Integer) m1.get(name)).compareTo((Integer) m2.get(name));
		} else if (m1.get(name) instanceof Double) {
			return ((Double) m1.get(name)).compareTo((Double) m2.get(name));
		}
		return 0;
	}


}
