package org.gooru.insights.api.controllers;

import java.io.IOException;
import java.text.ParseException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.services.ClassV2Service;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value="/v2/")
public class ClassV2Controller extends BaseController {

	@Autowired
	private ClassV2Service classService;

	private ClassV2Service getClassService() {
		return classService;
	}
	
	@RequestMapping(value = "/class/{classGooruId}/user/{userUid}/current/location", method = { RequestMethod.GET, RequestMethod.POST })
	public ModelAndView getUserCurrentLocationInLesson(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="userUid") String userUid,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getUserCurrentLocationInLesson(userUid, classGooruId));
	}
	
	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/peers", method = { RequestMethod.GET, RequestMethod.POST })
	public ModelAndView getCoursePeers(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getUserPeers(classGooruId, courseGooruId, null, null));
	}
	
	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/peers", method = { RequestMethod.GET, RequestMethod.POST })
	public ModelAndView getUnitPeers(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId,
			@PathVariable(value="unitGooruId") String unitGooruId,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getUserPeers(classGooruId, courseGooruId, unitGooruId, null));
	}
	
	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/peers", method = { RequestMethod.GET, RequestMethod.POST })
	public ModelAndView getLessonPeers(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId,
			@PathVariable(value="unitGooruId") String unitGooruId,
			@PathVariable(value="lessonGooruId") String lessonGooruId,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getUserPeers(classGooruId, courseGooruId, unitGooruId, lessonGooruId));
	}
	
}
