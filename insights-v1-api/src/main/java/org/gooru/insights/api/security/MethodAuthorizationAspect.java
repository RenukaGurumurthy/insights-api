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
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.services.CassandraService;
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

import com.datastax.driver.core.ResultSet;


@Aspect
public class MethodAuthorizationAspect extends OperationAuthorizer {

	@Autowired
	private RedisService redisService;
			
	@Autowired
	private CassandraService cassandraService;
	
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
				result = redisService.getDirectValue(sessionToken);
				if (result == null || result.isEmpty()) {
					InsightsLogger.error("null value in redis data for " + sessionToken);
					return false;
				}
				JSONObject jsonObject = new JSONObject(result);
				return isValidUser(jsonObject, request);
			} catch (Exception e) {
				InsightsLogger.error("Exception from redis:"+sessionToken, e);
				return false;
			}
		} else {
			throw new AccessDeniedException("sessionToken can not be NULL!");
		}
	}
	
	
	private boolean isValidUser(JSONObject jsonObject, HttpServletRequest request) {
		try {
			String userUidFromSession = jsonObject.getString(ApiConstants._USER_ID);
			String userIdFromRequest = RequestUtils.getUserIdFromRequestParam(request);
			String classId = RequestUtils.getClassIdFromRequestParam(request);
			if (StringUtils.isBlank(classId) || StringUtils.isBlank(userIdFromRequest)) {
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
			} else if (StringUtils.isNotBlank(classId)) {
				return isAuthorizedUser(classId, userUidFromSession);
			}

		} catch (Exception e) {
			InsightsLogger.error("Exception", e);
		}
		return false;
	}

	private boolean isAuthorizedUser(String classGooruId, String userUidFromSession) {
		if (StringUtils.isNotBlank(classGooruId)) {
			ResultSet authorizedUsers =  cassandraService.getAuthorizedUsers(classGooruId);
			if(authorizedUsers == null){
				LOG.error("API consumer is not a teacher or collaborator...");
				return false;
			}
			if(userUidFromSession.equalsIgnoreCase(authorizedUsers.one().getString(ApiConstants._CREATOR_UID))){
				return true;
			}
			 Set<Set> collaborators = authorizedUsers.one().getSet(ApiConstants.COLLABORATORS, Set.class);
			if(collaborators != null && collaborators.contains(userUidFromSession)){
				return true;
			}
		}
		//It should return false here. Re-look at this value once event sync implementation completed
		return true;
	}

}
