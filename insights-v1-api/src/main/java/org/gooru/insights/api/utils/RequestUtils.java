package org.gooru.insights.api.utils;

import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.spring.exception.BadRequestException;
import org.json.JSONException;
import org.json.JSONObject;

public final class RequestUtils {

	private static String id;

	public RequestUtils() {
		UUID uuid = UUID.randomUUID();
		id = uuid.toString();
	}

	public static String getSessionToken(HttpServletRequest request) {

		if (request.getHeader(ApiConstants.GOORU_SESSION_TOKEN) != null) {
			return request.getHeader(ApiConstants.GOORU_SESSION_TOKEN);
		} else {
			return request.getParameter(ApiConstants.SESSION_TOKEN);
		}
	}

	public static String getRequestData(HttpServletRequest request,String requestBody){
		requestBody = request.getParameter(ApiConstants.DATA) != null ? request.getParameter(ApiConstants.DATA) : requestBody;
			if(StringUtils.isBlank(requestBody)){
				throw new BadRequestException(ErrorMessages.E104.replace(ApiConstants.REPLACER, ApiConstants.DATA));
			}
			return requestBody;
	}

	public static void logRequest(HttpServletRequest request) {
		@SuppressWarnings("unchecked")
		Map<String, Object> requestParam = (Map<String,Object>)request.getParameterMap();
		JSONObject jsonObject = new JSONObject();
		try {
			for (Map.Entry<String, Object> entry : requestParam.entrySet()) {
					jsonObject.put(entry.getKey(), entry.getValue());
			}
			jsonObject.put("url", request.getRequestURL().toString());
			jsonObject.put("sessionToken", getSessionToken(request));
			InsightsLogger.debug(jsonObject.toString());
		} catch (JSONException e) {
			InsightsLogger.debug(jsonObject.toString(),e);
		}
	}

	public static String getId() {
		return id;
	}

	public static String getUserIdFromRequestParam(HttpServletRequest request) {
		return request.getParameter(ApiConstants.USERUID);
	}

	public static String getClassIdFromRequestParam(HttpServletRequest request) {
		return request.getParameter(ApiConstants.CLASS_GOORU_ID);
	}
	public static String getSessionIdFromRequestParam(HttpServletRequest request) {
		return request.getParameter(ApiConstants.SESSIONID);
	}
}
