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

import java.util.Properties;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.gooru.insights.api.models.User;
import org.json.JSONObject;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;


@Aspect
public class MethodAuthorizationAspect extends OperationAuthorizer {

	private static final Logger logger = LoggerFactory.getLogger(MethodAuthorizationAspect.class);
	
	@Resource(name = "gooruConstants")
	private Properties gooruConstants;

	private String GOORU_API_REST_END_POINT;

	@Around("accessCheckPointcut() && @annotation(authorizeOperations) && @annotation(requestMapping)")
	public Object operationsAuthorization(ProceedingJoinPoint pjp, AuthorizeOperations authorizeOperations, RequestMapping requestMapping) throws Throwable {

		// Check method access
		boolean permitted = hasOperationsAuthority(authorizeOperations, pjp);
		
		if (permitted) {
			return pjp.proceed();
		} else {
			throw new AccessDeniedException("Permission Denied");
		}
	}

	@Pointcut("execution(* org.gooru.insights.api.controllers.*.*(..))")
	public void accessCheckPointcut() {
	}
	
	public boolean hasOperationsAuthority(AuthorizeOperations authorizeOperations, ProceedingJoinPoint pjp) {
		
		HttpServletRequest request = null;
		HttpSession session = null;
		String sessionToken = null;
		Representation representation = null;
		JSONObject jsonObj = null;
		User user = null;
		
		if (RequestContextHolder.getRequestAttributes() != null) {
		request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
		session = request.getSession(true);
		}
		
		if(session.getAttribute("token") != null)
		{
			if(request.getParameter("sessionToken") != null){
				sessionToken = (String) session.getAttribute("token");
				if(sessionToken.equalsIgnoreCase(request.getParameter("sessionToken"))) {
				logger.info("Token : \n"+ sessionToken );
				return true;
				}
				else{
					return false;
				}
			}
		} 
		else {
			try {
				if(request.getParameter("sessionToken") != null){
					sessionToken = request.getParameter("sessionToken");
					logger.info("sessionToken : \n"+ sessionToken );
					GOORU_API_REST_END_POINT = gooruConstants
							.getProperty("gooru.api.rest.endpoint");
					String address = GOORU_API_REST_END_POINT + "/v2/user/token/"
							+ sessionToken + "?sessionToken=" + sessionToken;
					
					session.setAttribute("token", sessionToken);

					ClientResource client = new ClientResource(address);

					if (client.getStatus().isSuccess()) {
						representation = client.get();
						JsonRepresentation jsonRepresentation = new JsonRepresentation(
								representation);
						jsonObj = jsonRepresentation.getJsonObject();
						user = new User();
						user.setFirstName(jsonObj.getString("firstName"));
						user.setLastName(jsonObj.getString("lastName"));
						user.setEmailId(jsonObj.getString("emailId"));
						user.setGooruUId(jsonObj.getString("gooruUId"));
						
						if(user != null){
							return true;
						}
					}
				}
				else{
					return false;
				}
				
			} catch (Exception e) {
				logger.info("Invalid session token" + e);
			}
		}
		return false;
	}
	
}
