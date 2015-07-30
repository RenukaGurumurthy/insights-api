package org.gooru.insights.api.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;

public class ServiceUtils {

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
}
