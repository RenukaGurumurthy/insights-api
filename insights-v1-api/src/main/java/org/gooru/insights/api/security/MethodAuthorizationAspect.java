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
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.models.InsightsConstant.ColumnFamily;
import org.gooru.insights.api.models.User;
import org.gooru.insights.api.services.CassandraService;
import org.gooru.insights.api.services.RedisService;
import org.gooru.insights.api.utils.InsightsLogger;
import org.gooru.insights.api.utils.RequestUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Form;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;


@Aspect
public class MethodAuthorizationAspect extends OperationAuthorizer {

	@Resource(name = "gooruConstants")
	private Properties gooruConstants;

	@Autowired
	private RedisService redisService;
	
	@Autowired
	private CassandraService cassandraService;
	
	private ColumnList<String> endPoint;
	
	private ColumnList<String> entityOperationsRole;
	
	private static final String GOORU_PREFIX = "authenticate_";
	
	private static final String ASPECT = "aspect";
	
	private static final String DO_API = "doAPI";
	
	private static final String PROCEED = "proceed";
	
	@PostConstruct
	private void init(){
		 endPoint = cassandraService.getDashBoardKeys(ApiConstants.GOORU_REST_ENDPOINT);
		 entityOperationsRole = cassandraService.getDashBoardKeys(ApiConstants.ENTITY_ROLE_OPERATIONS);
	}
	
	@Around("accessCheckPointcut() && @annotation(authorizeOperations) && @annotation(requestMapping)")
	public Object operationsAuthorization(ProceedingJoinPoint pjp, AuthorizeOperations authorizeOperations, RequestMapping requestMapping) throws Throwable {

		// Check method access
		boolean permitted = validateApi(authorizeOperations, pjp);
		if (permitted) {
			return pjp.proceed();
		} else {
			throw new AccessDeniedException("Permission Denied! You don't have access");
		}
	}

	@Pointcut("execution(* org.gooru.insights.api.controllers.*.*(..))")
	public void accessCheckPointcut() {
	}
	
	
	private boolean validateApi(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp){
		Map<String,Boolean> isValid = hasRedisOperations(authorizeOperations,pjp);	
		if(isValid.get(DO_API)){
			InsightsLogger.info("doing API request");
			return hasApiOperationsAuthority(authorizeOperations,pjp);
		}
		return isValid.get(PROCEED);
	}
	
	private Map<String, Boolean> hasRedisOperations(
			AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {

		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		Map<String, Boolean> validStatus = new HashMap<String, Boolean>();
		validStatus.put(PROCEED, false);
		validStatus.put(DO_API, false);
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
					validStatus.put(DO_API, true);
					return validStatus;
				}
				JSONObject jsonObject = new JSONObject(result);
				jsonObject = new JSONObject(
						jsonObject.getString(ApiConstants.USER_TOKEN));
				jsonObject = new JSONObject(
						jsonObject.getString(ApiConstants.USER));
				User user = new User();
				user.setFirstName(jsonObject.getString(ApiConstants.FIRST_NAME));
				user.setLastName(jsonObject.getString(ApiConstants.LAST_NAME));
				user.setEmailId(jsonObject.getString(ApiConstants.EMAIL_ID));
				user.setGooruUId(jsonObject.getString(ApiConstants.PARTY_UId));
				if (hasGooruAdminAuthority(authorizeOperations, jsonObject)) {
					session.setAttribute(ApiConstants.SESSION_TOKEN,
							sessionToken);
					validStatus.put(PROCEED, true);
					return validStatus;
				}
				if (hasAuthority(authorizeOperations, jsonObject,request)) {
					session.setAttribute(ApiConstants.SESSION_TOKEN,
							sessionToken);
					validStatus.put(PROCEED, true);
					return validStatus;
				}
			} catch (Exception e) {
				InsightsLogger.error("Exception from redis:" + e.getMessage());
				validStatus.put(DO_API, true);
				return validStatus;
			}
		} else {
			throw new AccessDeniedException("sessionToken can not be NULL!");
		}
		return validStatus;
	}
	
	private boolean hasApiOperationsAuthority(
			AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {

		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		Map<String, Boolean> validStatus = new HashMap<String, Boolean>();
		validStatus.put(PROCEED, false);
		if (RequestContextHolder.getRequestAttributes() != null) {
			request = ((ServletRequestAttributes) RequestContextHolder
					.getRequestAttributes()).getRequest();
			session = request.getSession(true);
			sessionToken = RequestUtils.getSessionToken(request);
		}
		if (sessionToken != null) {
			String address = endPoint.getColumnByName(
					ApiConstants.CONSTANT_VALUE).getStringValue()
					+ "/v2/user/token/"
					+ sessionToken
					+ "?sessionToken="
					+ sessionToken;
			ClientResource client = new ClientResource(address);
			Form headers = (Form) client.getRequestAttributes().get(
					"org.restlet.http.headers");
			if (headers == null) {
				headers = new Form();
			}
			headers.add(ApiConstants.GOORU_SESSION_TOKEN, sessionToken);
			client.getRequestAttributes().put("org.restlet.http.headers",
					headers);
			if (client.getStatus().isSuccess()) {
				try {
					Representation representation = client.get();
					JsonRepresentation jsonRepresentation = new JsonRepresentation(
							representation);
					JSONObject jsonObj = jsonRepresentation.getJsonObject();
					User user = new User();
					user.setFirstName(jsonObj
							.getString(ApiConstants.FIRST_NAME));
					user.setLastName(jsonObj.getString(ApiConstants.LAST_NAME));
					user.setEmailId(jsonObj.getString(ApiConstants.EMAIL_ID));
					user.setGooruUId(jsonObj.getString(ApiConstants.GOORU_U_ID));
					if (hasGooruAdminAuthority(authorizeOperations, jsonObj)
							|| hasAuthority(authorizeOperations, jsonObj,request)) {
						session.setAttribute(ApiConstants.SESSION_TOKEN,
								sessionToken);
						return true;
					}
				} catch (Exception e) {
					throw new AccessDeniedException("Invalid sessionToken!");
				}
			} else {
				throw new AccessDeniedException("Invalid sessionToken!");
			}
		} else {
			InsightsLogger.debug("session token is null");
			throw new AccessDeniedException("sessionToken can not be NULL!");
		}
		return false;
	}
	
	private boolean hasGooruAdminAuthority(AuthorizeOperations authorizeOperations,JSONObject jsonObj){
		boolean roleAuthority = false;
		try {
			String userRole = jsonObj.getString(ApiConstants.USER_ROLE_SETSTRING);
			String operations =  authorizeOperations.operations();
			for(String op :operations.split(ApiConstants.COMMA)){
				if(userRole.contains(ApiConstants.ROLE_GOORU_ADMIN) || userRole.contains(ApiConstants.CONTENT_ADMIN) || userRole.contains(ApiConstants.ORGANIZATION_ADMIN)){
					roleAuthority = true;
				}
			}
		} catch (JSONException e) {
			InsightsLogger.debug("user doesn't have authorization to access:",e);
		}
		return roleAuthority;
		
	}
	
	private boolean hasAuthority(AuthorizeOperations authorizeOperations,JSONObject jsonObj,HttpServletRequest request){
			String userRole = null;
			try {
				userRole = jsonObj.getString(ApiConstants.USER_ROLE_SETSTRING);
			} catch (JSONException e) {
				InsightsLogger.error("unable to fetch the userRoleSetString:", e);
				return  false;
			}
			String operations =  authorizeOperations.operations();
			for(String op : operations.split(",")){
				String roles = entityOperationsRole.getColumnByName(op).getStringValue();
				InsightsLogger.debug("role : " +roles + "roleAuthority > :"+userRole.contains(roles));
				for(String role : roles.split(",")){
					if((userRole.contains(role))){
						if(operations.equalsIgnoreCase("Class~View")){
							return isValidUser(jsonObj, request);
						}else{
							return true;
						}
					}
				}
			}
		return false;
	}
	
	private boolean isValidUser(JSONObject jsonObject, HttpServletRequest request) {
		try {
			String userUidFromSession = jsonObject.getString(ApiConstants.PARTY_UId);
			String userIdFromRequest = RequestUtils.getUserIdFromRequestParam(request);
			String classId = null;
			String pathInfo = request.getPathInfo();
			if (pathInfo.startsWith("/class/")) {
				String[] pathParts = pathInfo.split("/");
				classId = pathParts[2];
			} else {
				classId = RequestUtils.getClassIdFromRequestParam(request);
			}
			if (StringUtils.isBlank(userIdFromRequest) && "ANONYMOUS".equalsIgnoreCase(userUidFromSession)) {
				InsightsLogger.info("ANONYMOUS user can't be a teacher class creator");
				return false;
			} else if (userIdFromRequest.equalsIgnoreCase(userUidFromSession)) {
				InsightsLogger.info("Session Token and user uid that is available in url is mismatching..");
				return true;
			} else if (StringUtils.isNotBlank(classId) && userUidFromSession.equalsIgnoreCase(getTeacherUid(classId))) {
				InsightsLogger.info("User is not a valid Class creatior or teacher..");
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
			try {
				OperationResult<ColumnList<String>> classData = cassandraService.read(ColumnFamily.CLASS.getColumnFamily(), classGooruId);
				if (!classData.getResult().isEmpty() && classData.getResult().size() > 0) {
					teacherUid = classData.getResult().getStringValue(ApiConstants._CREATOR_UID, null);
				}
			} catch (Exception e) {
				InsightsLogger.error("Exception", e);
			}
		}
		return teacherUid;
	}
}
