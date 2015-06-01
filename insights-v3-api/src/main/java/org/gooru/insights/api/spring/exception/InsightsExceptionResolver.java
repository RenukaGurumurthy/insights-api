/////////////////////////////////////////////////////////////
//GooruExceptionResolver.java
//rest-v2-app
// Created by Gooru on 2014
// Copyright (c) 2014 Gooru. All rights reserved.
// http://www.goorulearning.org/
// Permission is hereby granted, free of charge, to any person      obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so,  subject to
// the following conditions:
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY  KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE    WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR  PURPOSE     AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR  COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
/////////////////////////////////////////////////////////////
package org.gooru.insights.api.spring.exception;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.Numbers;
import org.gooru.insights.api.constants.ApiConstants.ModelAttributes;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.utils.InsightsLogger;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.SimpleMappingExceptionResolver;

import flexjson.JSONSerializer;

public class InsightsExceptionResolver extends SimpleMappingExceptionResolver {

	private static final String DEFAULT_ERROR = "Internal Server Error!!!";
	
	private static final String DEVELOPER_MESSAGE = "developerMessage";

	private static final String MAIL_To = "mailTo";

	private static final String SUPPORT_EMAIL_ID = "support@goorulearning.org";

	private static final String STATUS_CODE = "statusCode";
	
	private static final String DEFAULT_TRACEID = "traceId Not-Specified";

	public ModelAndView doResolveException(HttpServletRequest request,
			HttpServletResponse response, Object handler, Exception ex) {
		ResponseParamDTO<Map<Object, Object>> responseDTO = new ResponseParamDTO<Map<Object, Object>>();
		Map<Object, Object> errorMap = new HashMap<Object, Object>();
		Integer statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
		String traceId = request.getAttribute("traceId") != null ? request.getAttribute("traceId").toString() : DEFAULT_TRACEID;
		if (ex instanceof BadRequestException) {
			statusCode = HttpServletResponse.SC_BAD_REQUEST;
		} else if (ex instanceof AccessDeniedException) {
			statusCode = HttpServletResponse.SC_FORBIDDEN;
		}else {
			statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
		}

		if (statusCode.toString().startsWith(Numbers.FOUR.getNumber())) {
			InsightsLogger.debug(traceId, ex);
			errorMap.put(DEVELOPER_MESSAGE, ex.getMessage());
		} else if (statusCode.toString().startsWith(Numbers.FIVE.getNumber())) {
			InsightsLogger.error(traceId, ex);
			errorMap.put(DEVELOPER_MESSAGE, DEFAULT_ERROR);
		}
		errorMap.put(STATUS_CODE, statusCode);
		errorMap.put(MAIL_To, SUPPORT_EMAIL_ID);

		response.setStatus(statusCode);
		responseDTO.setMessage(errorMap);
		return new ModelAndView(ModelAttributes.VIEW_NAME.getAttribute(),
				ModelAttributes.RETURN_NAME.getAttribute(),
				new JSONSerializer().exclude(ApiConstants.EXCLUDE_CLASSES)
						.deepSerialize(responseDTO));

	}
}
