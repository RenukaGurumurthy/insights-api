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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
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
	//TODO @AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public ModelAndView getUserCurrentLocationInLesson(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="userUid") String userUid,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getUserCurrentLocationInLesson(userUid, classGooruId));
	}
	
	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/peers", method = { RequestMethod.GET, RequestMethod.POST })
	//TODO @AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public ModelAndView getCoursePeers(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getUserPeers(classGooruId, courseGooruId, null, null));
	}
	
	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/peers", method = { RequestMethod.GET, RequestMethod.POST })
	//TODO @AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public ModelAndView getUnitPeers(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId,
			@PathVariable(value="unitGooruId") String unitGooruId,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getUserPeers(classGooruId, courseGooruId, unitGooruId, null));
	}
	
	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/peers", method = { RequestMethod.GET, RequestMethod.POST })
	//TODO @AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public ModelAndView getLessonPeers(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId,
			@PathVariable(value="unitGooruId") String unitGooruId,
			@PathVariable(value="lessonGooruId") String lessonGooruId,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getUserPeers(classGooruId, courseGooruId, unitGooruId, lessonGooruId));
	}
	
	@RequestMapping(value = "{collectionType}/{contentGooruId}/user/{userUId}/session/{sessionId}/status", method = {RequestMethod.GET, RequestMethod.POST })
	//TODO @AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getSessionStatus(HttpServletRequest request, @PathVariable(value = "userUId") String userUId,
			@PathVariable(value = "sessionId") String sessionId,
			@PathVariable(value = "contentGooruId") String contentGooruId,
			@PathVariable(value = "collectionType") String collectionType, HttpServletResponse response)
					throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getSessionStatus(contentGooruId, userUId, sessionId));
	}
	
	@RequestMapping(value="/{collectionType}/{contentGooruId}/sessions",method ={ RequestMethod.GET,RequestMethod.POST})
	//TODO @AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getUserSessions(HttpServletRequest request, 
			@RequestParam(value="classGooruId", required = false) String classGooruId,
			@RequestParam(value="courseGooruId", required = false) String courseGooruId, 
			@RequestParam(value="unitGooruId", required = false) String unitGooruId, 
			@RequestParam(value="lessonGooruId", required = false) String lessonGooruId,
			@PathVariable(value="collectionType") String collectionType, 
			@PathVariable(value="contentGooruId") String contentGooruId,
			@RequestParam(value="userUid", required = true) String userUid,
			HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getUserSessions(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, collectionType, userUid));
	}
	
	@RequestMapping(value="/{collectionType}/{contentGooruId}/user/{userUid}",method ={ RequestMethod.GET,RequestMethod.POST})
	// TODO @AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getStudentAssessmentData(HttpServletRequest request, 
			@PathVariable(value="collectionType") String collectionType, @PathVariable(value="userUid") String userUid, 
			@RequestParam(value="sessionId", required = false) String sessionId, @RequestParam(value="classGooruId", required = false) String classGooruId,
			@RequestParam(value="courseGooruId", required = false) String courseGooruId, @RequestParam(value="unitGooruId", required = false) String unitGooruId, 
			@RequestParam(value="lessonGooruId", required = false) String lessonGooruId, @PathVariable(value="contentGooruId") String contentGooruId,
			HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getSummaryData(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, sessionId, userUid, collectionType));
	}

	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/performance", method = { RequestMethod.GET, RequestMethod.POST })
	//TODO @AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public ModelAndView getUserCoursePerformance(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value = "courseGooruId") String courseGooruId,
			@RequestParam(value = "userUid", required = false) String userUid,
			@RequestParam(value = "collectionType", required = true) String collectionType,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getPerformanceData(classGooruId, courseGooruId, null, null, userUid, collectionType));
	}
	
	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/performance", method = { RequestMethod.GET, RequestMethod.POST })
	//TODO @AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public ModelAndView getUserUnitPerformance(HttpServletRequest request, 
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value = "courseGooruId") String courseGooruId,
			@PathVariable(value = "unitGooruId") String unitGooruId,
			@RequestParam(value = "userUid", required = false) String userUid,
			@RequestParam(value = "collectionType", required = true) String collectionType,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getPerformanceData(classGooruId, courseGooruId, unitGooruId, null, userUid, collectionType));
	}
	
	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/performance", method = { RequestMethod.GET, RequestMethod.POST })
	//TODO @AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public ModelAndView getUserLessonPerformance(HttpServletRequest request, 
			@PathVariable(value = "classGooruId") String classGooruId,
			@PathVariable(value = "courseGooruId") String courseGooruId,
			@PathVariable(value = "unitGooruId") String unitGooruId,
			@PathVariable(value = "lessonGooruId") String lessonGooruId,
			@RequestParam(value = "userUid", required = false) String userUid,
			@RequestParam(value = "collectionType", required = true) String collectionType,
			HttpServletResponse response) throws JSONException, ParseException, IOException {
		setAllowOrigin(response);
		return getModel(getClassService().getPerformanceData(classGooruId, courseGooruId, unitGooruId, lessonGooruId, userUid, collectionType));
	}
}
