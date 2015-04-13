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

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.gooru.insights.api.models.User;
import org.gooru.insights.api.services.CassandraService;
import org.gooru.insights.api.services.RedisService;
import org.gooru.insights.api.utils.InsightsLogger;
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
	
	private static final String TRACE_ID = "aspect";
	
	private static final String TOKEN_HEADER_PREFIX = "Gooru-Session-Token";
	
	private static final String TOKEN_PARAM_PREFIX = "sessionToken";

	@PostConstruct
	private void init(){
		 endPoint = cassandraService.getDashBoardKeys("tomcat-init","gooru.api.rest.endpoint");
		 entityOperationsRole = cassandraService.getDashBoardKeys("tomcat-init","entity_role_opertaions");
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
		if(isValid.get("doApi")){
			InsightsLogger.info(TRACE_ID, "doing API request");
			return hasApiOperationsAuthority(authorizeOperations,pjp);
		}
		return isValid.get("proceed");
	}
	
	private Map<String,Boolean> hasRedisOperations(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {
		
		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		Map<String,Boolean> validStatus = new HashMap<String,Boolean>();
		validStatus.put("proceed", false);
		validStatus.put("doApi", false);
		if (RequestContextHolder.getRequestAttributes() != null) {
		request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
		session = request.getSession(true);
		request.setAttribute("traceId", TRACE_ID);
		}
		sessionToken = getSessionToken(request);
		
		if(sessionToken != null){
			String result;
			try{
			result = redisService.getDirectValue(GOORU_PREFIX+sessionToken);
			if(result == null || result.isEmpty()){
				InsightsLogger.error(TRACE_ID, "null value in redis data for "+GOORU_PREFIX+sessionToken);
				validStatus.put("doApi", true);
				return validStatus;
			}
				JSONObject	jsonObject = new JSONObject(result);
				jsonObject = new JSONObject(jsonObject.getString("userToken"));
				jsonObject = new JSONObject(jsonObject.getString("user"));
				User user = new User();
				user.setFirstName(jsonObject.getString("firstName"));
				user.setLastName(jsonObject.getString("lastName"));
				user.setEmailId(jsonObject.getString("emailId"));
				user.setGooruUId(jsonObject.getString("partyUid"));
				if(hasGooruAdminAuthority(authorizeOperations, jsonObject)){
					session.setAttribute("sessionToken", sessionToken);
					validStatus.put("proceed", true);
					return validStatus;
				}
				 if(hasAuthority(authorizeOperations, jsonObject)){
					 session.setAttribute("sessionToken", sessionToken);
					 validStatus.put("proceed", true);
					 return validStatus;
				 }
				} catch (Exception e) {
					InsightsLogger.error(TRACE_ID, "Exception from redis:"+e.getMessage());
					validStatus.put("doApi", true);
					return validStatus;
				}
	}else{
		throw new AccessDeniedException("sessionToken can not be NULL!");
	}
		return validStatus;
	}
	
	private boolean hasApiOperationsAuthority(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {
		
		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		Map<String,Boolean> validStatus = new HashMap<String,Boolean>();
		validStatus.put("proceed", false);
		if (RequestContextHolder.getRequestAttributes() != null) {
		request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
		session = request.getSession(true);
		}
		sessionToken = getSessionToken(request);
				if(sessionToken != null){
					String address = endPoint.getColumnByName("constant_value").getStringValue()+"/v2/user/token/"+ sessionToken + "?sessionToken=" + sessionToken;
					ClientResource client = new ClientResource(address);
					Form headers = (Form)client.getRequestAttributes().get("org.restlet.http.headers");
					if (headers == null) {
					    headers = new Form();
					}
					    headers.add("Gooru-Session-Token", sessionToken);
					    client.getRequestAttributes().put("org.restlet.http.headers", headers);
					if (client.getStatus().isSuccess()) {
						try{
							Representation representation = client.get();
							JsonRepresentation jsonRepresentation = new JsonRepresentation(
									representation);
							JSONObject jsonObj = jsonRepresentation.getJsonObject();
							User user = new User();
							user.setFirstName(jsonObj.getString("firstName"));
							user.setLastName(jsonObj.getString("lastName"));
							user.setEmailId(jsonObj.getString("emailId"));
							user.setGooruUId(jsonObj.getString("gooruUId"));
							if(hasGooruAdminAuthority(authorizeOperations, jsonObj)){
								session.setAttribute("token", sessionToken);
								return true;
							}
							 if(hasAuthority(authorizeOperations, jsonObj)){
								 session.setAttribute("token", sessionToken);
								 return true;
							 }
						}catch(Exception e){
							throw new AccessDeniedException("Invalid sessionToken!");
						}
					}else{
						throw new AccessDeniedException("Invalid sessionToken!");
					}
				}else{
					InsightsLogger.debug(TRACE_ID, "session token is null");
					throw new AccessDeniedException("sessionToken can not be NULL!");
				}
				return false;
		}
	
	private boolean hasGooruAdminAuthority(AuthorizeOperations authorizeOperations,JSONObject jsonObj){
		boolean roleAuthority = false;
		try {
			String userRole = jsonObj.getString("userRoleSetString");
			String operations =  authorizeOperations.operations();
			for(String op :operations.split(",")){
				if(userRole.contains("ROLE_GOORU_ADMIN") || userRole.contains("Content_Admin") || userRole.contains("Organization_Admin")){
					roleAuthority = true;
				}
			}
		} catch (JSONException e) {
			InsightsLogger.debug(TRACE_ID,"user doesn't have authorization to access:",e);
		}
		return roleAuthority;
		
	}
	
	private boolean hasAuthority(AuthorizeOperations authorizeOperations,JSONObject jsonObj){
			String userRole = null;
			try {
				userRole = jsonObj.getString("userRoleSetString");
			} catch (JSONException e) {
				InsightsLogger.error(TRACE_ID,"unable to fetch the userRoleSetString:", e);
				return  false;
			}
			String operations =  authorizeOperations.operations();
			for(String op : operations.split(",")){
				String roles = entityOperationsRole.getColumnByName(op).getStringValue();
				InsightsLogger.debug(TRACE_ID, "role : " +roles + "roleAuthority > :"+userRole.contains(roles));
				for(String role : roles.split(",")){
					if((userRole.contains(role))){
						return true;
					}
				}
			}
		return false;
	}
	
	private String getSessionToken(HttpServletRequest request) {

		if (request.getHeader(TOKEN_HEADER_PREFIX) != null) {
			return request.getHeader(TOKEN_HEADER_PREFIX);
		} else {
			return request.getParameter(TOKEN_PARAM_PREFIX);
		}
	}
}
