package org.gooru.insights.api.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.services.BaseService;
import org.gooru.insights.api.services.ClassService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value="class/")
public class ClassController extends BaseController{

	@Autowired
	private ClassService classService;
	
	@Autowired
	private BaseService baseService;
	
	private ClassService getClassService() {
		return classService;
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/unit",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getClasspageCourseUsage(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @RequestParam(value="getUsageData", required = false) Boolean getUsageData,
			@RequestParam(value="userUid", required = false) String userUid, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getCourseUsage(getTraceId(request), classGooruId, courseGooruId, userUid, getUsageData, request.isSecure()));
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getClasspageUnitUsage(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId, 
			@RequestParam(value="userUid", required = false) String userUid,
			@RequestParam(value="collectionType", required = false) String collectionType,
			@RequestParam(value="getUsageData", required = false) Boolean getUsageData, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getUnitUsage(getTraceId(request), classGooruId, courseGooruId, unitGooruId, userUid,collectionType, getUsageData, request.isSecure()));
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getClasspageLessonUsage(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId, 
			@PathVariable(value="lessonGooruId") String lessonGooruId, @RequestParam(value="userUid", required = false) String userUid,
			@RequestParam(value="getUsageData", required = false) Boolean getUsageData, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getLessonUsage(getTraceId(request), classGooruId, courseGooruId, unitGooruId, lessonGooruId, userUid, getUsageData, request.isSecure()));
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/plan",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getCoursePlan(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @RequestParam(value="userUid", required = true) String userUid, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getCoursePlan(getTraceId(request), classGooruId, courseGooruId, userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/plan",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getUnitPlan(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId,
			@RequestParam(value="userUid", required = true) String userUid, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getUnitPlan(getTraceId(request), classGooruId, courseGooruId, unitGooruId, userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/progress",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getCourseProgress(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @RequestParam(value="userUid", required = true) String userUid, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getCourseProgress(getTraceId(request), classGooruId, courseGooruId, userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/progress",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getUnitProgress(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId,
			@RequestParam(value="userUid", required = true) String userUid, HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getUnitProgress(getTraceId(request), classGooruId, courseGooruId, unitGooruId, userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/plan",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getLessonPlan(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId,
			@PathVariable(value="lessonGooruId") String lessonGooruId, @RequestParam(value="assessmentIds") String assessmentIds,
			@RequestParam(value="userUid", required = true) String userUid, HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getLessonPlan(getTraceId(request), classGooruId, courseGooruId, unitGooruId, lessonGooruId, assessmentIds, userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/{collectionType}/{contentGooruId}/sessions",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getUserSessions(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId, 
			@PathVariable(value="lessonGooruId") String lessonGooruId,@PathVariable(value="collectionType") String collectionType, 
			@PathVariable(value="contentGooruId") String contentGooruId, @RequestParam(value="fetchOpenSession", required = false, defaultValue="false") boolean fetchOpenSession, 
			@RequestParam(value="userUid", required = true) String userUid,
			HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getUserSessions(getTraceId(request), classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, collectionType, userUid, fetchOpenSession, request.isSecure()));
	}
	
	@RequestMapping(value="/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/assessment/{contentGooruId}",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getStudentAssessmentData(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId, 
			@PathVariable(value="lessonGooruId") String lessonGooruId, @PathVariable(value="contentGooruId") String contentGooruId,
			@RequestParam(value="userUid", required = true) String userUid, 
			HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getStudentAssessmentData(getTraceId(request), classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, userUid, request.isSecure()));
	}
	
}
