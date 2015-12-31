package org.gooru.insights.api.controllers;

import java.io.IOException;
import java.text.ParseException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.services.ClassV2Service;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value="/v2/")
public class ClassV2Controller extends BaseController{

	@Autowired
	private ClassV2Service classService;

	private ClassV2Service getClassService() {
		return classService;
	}
	
	//TODO 	Test code to be removed
	@RequestMapping(value="/class/insert", method = {RequestMethod.GET})
	public void insertClass(HttpServletRequest request,
			HttpServletResponse response) throws JSONException, ParseException,
			IOException {

		getClassService().insertClassData();
	}
	
	@RequestMapping(value="{collectionType}/{contentGooruId}/user/{userUId}/session/{sessionId}/status",method ={ RequestMethod.GET,RequestMethod.POST})
	//@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getSessionStatus(HttpServletRequest request, @PathVariable(value="userUId") String userUId, @PathVariable(value="sessionId") String sessionId,@PathVariable(value="contentGooruId") String contentGooruId, @PathVariable(value="collectionType") String collectionType, 
			HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getSessionStatus(contentGooruId, userUId, sessionId));
	}
	
	@RequestMapping(value="/{collectionType}/{contentGooruId}/sessions",method ={ RequestMethod.GET,RequestMethod.POST})
	//@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getUserSessions(HttpServletRequest request, @RequestParam(value="classGooruId", required = false) String classGooruId,
			@RequestParam(value="courseGooruId", required = false) String courseGooruId, @RequestParam(value="unitGooruId", required = false) String unitGooruId, 
			@RequestParam(value="lessonGooruId", required = false) String lessonGooruId,@PathVariable(value="collectionType") String collectionType, 
			@PathVariable(value="contentGooruId") String contentGooruId,
			@RequestParam(value="userUid", required = true) String userUid,
			HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getUserSessions(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, collectionType, userUid));
	}
}
