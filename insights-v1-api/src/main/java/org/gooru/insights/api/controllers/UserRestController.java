package org.gooru.insights.api.controllers;

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
			HttpServletResponse response) throws Exception {
		return getModel(userservice.getPreferenceDataByType(data, userId));

	}
	
	@RequestMapping(value = "/top/preferences", method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView getUserDetails(HttpServletRequest request,  
			HttpServletResponse response) throws Exception {
		return getModel(userservice.getTopPreferenceList());
		
	}

	@RequestMapping(value = "/{userUid}/taxonomy/proficiency", method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView getUserTaxonomyProficiency(HttpServletRequest request, @PathVariable(value = "userUid") String userId, @RequestParam(value = DATA_OBJECT, required = false) String data,
			HttpServletResponse response) throws NumberFormatException, Exception {
		setAllowOrigin(response);
		return getModel(userservice.getProficiencyData(data, userId));

	}
	
	@RequestMapping(value = "/top/proficiency", method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView getUserProficiencyDetails(HttpServletRequest request,  
			HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(userservice.getTopProficiencyList());
		
	}
	
	@RequestMapping(method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView getUserData(HttpServletRequest request, @RequestParam(value = DATA_OBJECT,required = true) String data, HttpServletResponse response) throws Exception {
		return getModel(userservice.getUserData(data));

	}

}
