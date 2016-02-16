package org.gooru.insights.api.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ServiceUtils {

	private static Gson gson = new Gson();
	
	public static Collection<String> generateCommaSeparatedStringToKeys(
			String fields, String prefix, Collection<String> suffix) {
		Collection<String> resultFields = generateCommaSeparatedStringToKeys(
				fields, prefix, convertCollectionToString(suffix));
		return resultFields;
	}

	public static String convertCollectionToString(Collection<String> fields) {
		StringBuffer stringBuffer = new StringBuffer();
		for (String field : fields) {
			if (stringBuffer.length() > 0) {
				stringBuffer.append(ApiConstants.COMMA);
			}
			stringBuffer.append(field);
		}
		return stringBuffer.toString();
	}

	public static Collection<String> generateCommaSeparatedStringToKeys(
			String fields, String prefixes, String suffixes) {
		Collection<String> resultFields = new ArrayList<String>();
		if (StringUtils.isNotBlank(fields)) {
			if (StringUtils.isNotBlank(prefixes) && StringUtils.isNotBlank(suffixes)) {
				String prefixedKey = mergeCommaSeparatedKeys(prefixes, fields);
				resultFields.addAll(convertStringToCollection(mergeCommaSeparatedKeys(
								prefixedKey, suffixes)));
			} else if (StringUtils.isNotBlank(prefixes) && StringUtils.isBlank(suffixes)) {
				resultFields.addAll(convertStringToCollection(mergeCommaSeparatedKeys(
								prefixes, fields)));
			} else if (StringUtils.isBlank(prefixes) && StringUtils.isNotBlank(suffixes)) {
				resultFields.addAll(convertStringToCollection(mergeCommaSeparatedKeys(
								fields, suffixes)));
			} else {
				resultFields.addAll(convertStringToCollection(fields));
			}
		}
		return resultFields;
	}

	public static String mergeCommaSeparatedKeys(String firstFields,
			String secondFields) {
		StringBuffer stringBuffer = new StringBuffer();
		for (String firstField : firstFields.split(ApiConstants.COMMA)) {
			for (String secondField : secondFields.split(ApiConstants.COMMA)) {
				if (stringBuffer.length() > 0) {
					stringBuffer.append(ApiConstants.COMMA);
				}
				stringBuffer.append(firstField);
				stringBuffer.append(secondField);
			}
		}
		return stringBuffer.toString();
	}

	public static List<Map<String, Object>> insertRecordInToRecords(
			List<Map<String, Object>> records,
			Map<String, Object> injuctableRecord) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> record : records) {
			record.putAll(injuctableRecord);
			resultList.add(record);
		}
		return resultList;
	}

	/**
	 * 	Accumulate all the resource ids in single unit with List of Map and String as arguments 
	 */
	public static StringBuffer getCommaSeparatedIds(List<Map<String, Object>> dataList, String key) {
			StringBuffer exportData = new StringBuffer();
			if(dataList != null){
				for (Map<String, Object> map : dataList) {
						if (map.get(key) != null) {
							if(exportData.length() > 0) {
								exportData.append(ApiConstants.COMMA);
							} 
							exportData.append(map.get(key));
						}
				}
			}
			return exportData;
	}
	
	public static boolean notNull(String parameter) {

		if (StringUtils.trimToNull(parameter) != null) {
			return true;
		}
		return false;
	}

	public static boolean notNull(Map<?, ?> request) {

		if (request != null && (!request.isEmpty())) {
			return true;
		}
		return false;
	}

	public static boolean notNull(Integer parameter) {

		if (parameter != null && parameter.SIZE > 0
				&& (!parameter.toString().isEmpty())) {
			return true;
		}
		return false;
	}

	public static String buildString(Object... texts) {
		StringBuffer sb = new StringBuffer();
		for (Object text : texts) {
			sb.append(text);
		}
		return sb.toString();
	}

	public static Collection<String> convertStringToCollection(String field) {
		Collection<String> includedData = new ArrayList<String>();
		for (String value : field.split(ApiConstants.COMMA)) {
			includedData.add(value);
		}
		return includedData;
	}
	
	public static List<String> generateList(Object... objects){
		List<String> dataObject = new ArrayList<String>();
		for(int objectCount =0; objectCount<objects.length; objectCount++){
			dataObject.add(objects[objectCount].toString());
		}
		return dataObject;
	}
	
	public static String appendComma(String... texts) {
		StringBuffer sb = new StringBuffer();
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
		StringBuffer sb = new StringBuffer();
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
	
	public static String appendForwardSlash(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ApiConstants.FORWARD_SLASH);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
	
	public static Map<String,Object> castJSONToMap(String data) {
				return gson.fromJson(data, (new TypeToken<Map<String, Object>>() {}).getType());
	}
	
	public static List<Map<String, Object>> sortBy(List<Map<String, Object>> requestData, String sortBy, String sortOrder) {

		if (notNull(sortBy)) {
			for (final String name : sortBy.split(ApiConstants.COMMA)) {
				boolean descending = false;
				if (notNull(sortOrder) && sortOrder.equalsIgnoreCase(ApiConstants.DESC)) {
					descending = true;
				}
				if (descending) {
					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
							if (m2.containsKey(name)) {
								if (m1.containsKey(name)) {
									return compareTo(m2, m1, name);
								} else {
									return 1;
								}
							} else {
								return -1;
							}
						}
					});

				} else {
					Collections.sort(requestData, new Comparator<Map<String, Object>>() {
						public int compare(final Map<String, Object> m1, final Map<String, Object> m2) {
							if (m1.containsKey(name) && m2.containsKey(name)) {
								return compareTo(m1, m2, name);
							}
							return 1;
						}
					});
				}
			}
		}
		return requestData;
	}

	private static int compareTo(Map<String, Object> m1, Map<String, Object> m2, String name) {
		if (m1.get(name) instanceof String) {
			return ((String) m1.get(name).toString().toLowerCase()).compareTo((String) m2.get(name).toString().toLowerCase());
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
