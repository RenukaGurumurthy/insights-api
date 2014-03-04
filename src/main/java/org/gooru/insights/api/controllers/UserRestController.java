/*******************************************************************************
 * UserRestController.java
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
package org.gooru.insights.api.controllers;

import java.text.ParseException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value = "/user")
public class UserRestController extends BaseController implements InsightsConstant {

	@Autowired
	private UserService userservice;

	@RequestMapping(value = "/{userUid}/preference", method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView getUserPreference(HttpServletRequest request, @PathVariable(value = "userUid") String userId, @RequestParam(value = DATA_OBJECT, required = false) String data,
			HttpServletResponse response) throws ParseException {
		String databaseType = "cassandra";
		return getModel(userservice.getPreferenceDataByType(databaseType, data, userId, handleErrors()), handleErrors());

	}
	
	@RequestMapping(value = "/top/preferences", method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView getUserDetails(HttpServletRequest request,  
			HttpServletResponse response) throws ParseException {
		String databaseType = "cassandra";
		return getModel(userservice.getTopPreferenceList(databaseType, handleErrors()), handleErrors());
		
	}

	@RequestMapping(value = "/{userUid}/taxonomy/proficiency", method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView getUserTaxonomyProficiency(HttpServletRequest request, @PathVariable(value = "userUid") String userId, @RequestParam(value = DATA_OBJECT, required = false) String data,
			HttpServletResponse response) throws ParseException {
		String databaseType = "cassandra";
		return getModelObject(userservice.getProficiencyData(databaseType, data, userId, handleErrors()), handleErrors());

	}
	
	@RequestMapping(value = "/top/proficiency", method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView getUserProficiencyDetails(HttpServletRequest request,  
			HttpServletResponse response) throws ParseException {
		String databaseType = "cassandra";
		return getModel(userservice.getTopProficiencyList(databaseType, handleErrors()), handleErrors());
		
	}
	
	@RequestMapping(value = "/{username}", method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView getUserDetails(HttpServletRequest request, @PathVariable(value = "username") String userName, 
			HttpServletResponse response) throws ParseException {
		String databaseType = "cassandra";
		return getModel(userservice.getUserUid(databaseType, userName, handleErrors()), handleErrors());

	}

}
