/*******************************************************************************
 * MethodAuthorizationAspect.java
 * insights-read-api
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.gooru.insights.api.security;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.services.RedisService;
import org.gooru.insights.api.utils.InsightsLogger;
import org.gooru.insights.api.utils.RequestUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;


@Aspect
public class MethodAuthorizationAspect extends OperationAuthorizer {

	@Autowired
	private RedisService redisService;
			
	private Map<String, String> entityOperationsRole = new HashMap<String, String>();
	
	private static final String GOORU_PREFIX = "authenticate_";
		
	private static final Logger LOG = LoggerFactory.getLogger(MethodAuthorizationAspect.class);

	
	@PostConstruct
	private void init(){
		 entityOperationsRole.put(ApiConstants.REPORT + ApiConstants.TILDA + ApiConstants.VIEW, "ROLE_GOORU_ADMIN,superadmin,Organization_Admin,Content_Admin,User,user");
	}
	
	@Around("accessCheckPointcut() && @annotation(authorizeOperations) && @annotation(requestMapping)")
	public Object operationsAuthorization(ProceedingJoinPoint pjp, AuthorizeOperations authorizeOperations, RequestMapping requestMapping) throws Throwable {
		// Check method access
		boolean permitted = hasRedisOperations(authorizeOperations, pjp);
		if (permitted) {
			return pjp.proceed();
		} else {
			throw new AccessDeniedException("Permission Denied! You don't have access");
		}
	}

	@Pointcut("execution(* org.gooru.insights.api.controllers.*.*(..))")
	public void accessCheckPointcut() {
	}
	
	
	private boolean hasRedisOperations(
			AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {

		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		if (RequestContextHolder.getRequestAttributes() != null) {
			request = ((ServletRequestAttributes) RequestContextHolder
					.getRequestAttributes()).getRequest();
			session = request.getSession(true);
			sessionToken = RequestUtils.getSessionToken(request);
			RequestUtils.logRequest(request);
		}
		if (sessionToken != null) {
			String result;
			try {
				result = redisService.getDirectValue(GOORU_PREFIX
						+ sessionToken);
				if (result == null || result.isEmpty()) {
					InsightsLogger.error("null value in redis data for " + GOORU_PREFIX
									+ sessionToken);
					return false;
				}
				JSONObject jsonObject = new JSONObject(result);
				jsonObject = new JSONObject(
						jsonObject.getString(ApiConstants.USER_TOKEN));
				jsonObject = new JSONObject(
						jsonObject.getString(ApiConstants.USER));
				return isValidUser(jsonObject, request);
			} catch (Exception e) {
				InsightsLogger.error("Exception from redis:"+GOORU_PREFIX+sessionToken, e);
				return false;
			}
		} else {
			throw new AccessDeniedException("sessionToken can not be NULL!");
		}
	}
	
	
	private boolean isValidUser(JSONObject jsonObject, HttpServletRequest request) {
		try {
			String userUidFromSession = jsonObject.getString(ApiConstants.PARTY_UId);
			String userIdFromRequest = RequestUtils.getUserIdFromRequestParam(request);
			String classId = RequestUtils.getClassIdFromRequestParam(request);
			if (StringUtils.isNotBlank(classId) || StringUtils.isNotBlank(userIdFromRequest)) {
				String pathInfo = request.getPathInfo();
				int i = 0;
				String[] path = pathInfo.split("/");
				for (String pathVar : path) {
					if (pathVar.equalsIgnoreCase("user")) {
						userIdFromRequest = path[(i + 1)];
					}
					if (pathVar.equalsIgnoreCase("class")) {
						classId = path[(i + 1)];
					}
					i++;
				}
			}
			LOG.info("userUidFromSession : {} - userIdFromRequest {} - classId : {}",userUidFromSession,userIdFromRequest,classId);
			if (StringUtils.isBlank(userIdFromRequest) && "ANONYMOUS".equalsIgnoreCase(userUidFromSession)) {
				InsightsLogger.info("ANONYMOUS user can't be a teacher class creator");
				return false;
			} else if (StringUtils.isNotBlank(userIdFromRequest) && userIdFromRequest.equalsIgnoreCase(userUidFromSession)) {
				return true;
			} else if (StringUtils.isNotBlank(classId) && userUidFromSession.equalsIgnoreCase(getTeacherUid(classId))) {
				return true;
			}

		} catch (Exception e) {
			InsightsLogger.error("Exception", e);
		}
		return false;
	}

	private String getTeacherUid(String classGooruId) {
		String teacherUid = null;
		if (StringUtils.isNotBlank(classGooruId)) {
			/**
			 * Re-look at this area
			 * 
			try {
				OperationResult<ColumnList<String>> classData = cassandraService.read(ColumnFamilySet.CLASS.getColumnFamily(), classGooruId);
				if (!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
					teacherUid = classData.getResult().getStringValue(ApiConstants._CREATOR_UID, null);
				}
			} catch (Exception e) {
				InsightsLogger.error("Exception", e);
			}
		*/}
		return teacherUid;
	}

}
