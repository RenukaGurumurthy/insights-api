package org.gooru.insights.api.controllers;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.exporters.ClassExporterProcessor;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.services.BaseService;
import org.gooru.insights.api.services.ClassService;
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
@RequestMapping(value="/")
public class ClassController extends BaseController{

	@Autowired
	private ClassService classService;
	
	@Autowired
	private BaseService baseService;
	
	@Autowired
	ClassExporterProcessor classExporterProcessor;

	private ClassService getClassService() {
		return classService;
	}
	
	@RequestMapping(value="/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_VIEW)
	@ResponseBody
	public ModelAndView getClassUnitUsage(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId, 
			@RequestParam(value="userUid", required = false) String userUid,
			@RequestParam(value="collectionType", required = false) String collectionType,
			@RequestParam(value="getUsageData", required = false, defaultValue = "false") Boolean getUsageData, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getAllStudentsUnitUsage(classGooruId, courseGooruId, unitGooruId, userUid,collectionType, getUsageData, request.isSecure()));
	}
	
	@RequestMapping(value="/class/{classGooruId}/course/{courseGooruId}/plan",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_VIEW)
	@ResponseBody
	public ModelAndView getCoursePlan(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @RequestParam(value="userUid", required = true) String userUid, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getCoursePlan(classGooruId, courseGooruId, userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/plan",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_VIEW)
	@ResponseBody
	public ModelAndView getUnitPlan(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId,
			@RequestParam(value="userUid", required = true) String userUid, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getUnitPlan(classGooruId, courseGooruId, unitGooruId, userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/class/{classGooruId}/course/{courseGooruId}/progress",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_VIEW)
	@ResponseBody
	public ModelAndView getCourseProgress(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @RequestParam(value="userUid", required = false) String userUid, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getCourseProgress(classGooruId, courseGooruId,userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/progress",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_VIEW)
	@ResponseBody
	public ModelAndView getUnitProgress(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId,
			@RequestParam(value="userUid", required = true) String userUid, HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getUnitProgress(classGooruId, courseGooruId, unitGooruId, userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/usage",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_VIEW)
	@ResponseBody
	public ModelAndView getLessonAssessmentsUsage(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId,
			@PathVariable(value="lessonGooruId") String lessonGooruId, @RequestParam(value="contentGooruIds", required = true) String contentGooruIds,
			@RequestParam(value="userUid", required = true) String userUid, HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getLessonAssessmentsUsage(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruIds, userUid, request.isSecure()));
	}
	
	@RequestMapping(value="/{collectionType}/{contentGooruId}/sessions",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getUserSessions(HttpServletRequest request, @RequestParam(value="classGooruId", required = false) String classGooruId,
			@RequestParam(value="courseGooruId", required = false) String courseGooruId, @RequestParam(value="unitGooruId", required = false) String unitGooruId, 
			@RequestParam(value="lessonGooruId", required = false) String lessonGooruId,@PathVariable(value="collectionType") String collectionType, 
			@PathVariable(value="contentGooruId") String contentGooruId, @RequestParam(value="fetchOpenSession", required = false, defaultValue="false") boolean fetchOpenSession, 
			@RequestParam(value="userUid", required = true) String userUid,
			HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getModel(getClassService().getUserSessions(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, collectionType, userUid, fetchOpenSession, request.isSecure()));
	}
	
	@RequestMapping(value="/{collectionType}/{contentGooruId}/user/{userUid}",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getStudentAssessmentData(HttpServletRequest request, 
			@PathVariable(value="collectionType") String collectionType, @PathVariable(value="userUid") String userUid, 
			@RequestParam(value="sessionId", required = false) String sessionId, @RequestParam(value="classGooruId", required = false) String classGooruId,
			@RequestParam(value="courseGooruId", required = false) String courseGooruId, @RequestParam(value="unitGooruId", required = false) String unitGooruId, 
			@RequestParam(value="lessonGooruId", required = false) String lessonGooruId, @PathVariable(value="contentGooruId") String contentGooruId,
			HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getStudentAssessmentData(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, sessionId, userUid, collectionType, request.isSecure()));
	}
	
	@RequestMapping(value="{collectionType}/{contentGooruId}/session/{sessionId}/status",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getSessionStatus(HttpServletRequest request, @PathVariable(value="sessionId") String sessionId,@PathVariable(value="contentGooruId") String contentGooruId, @PathVariable(value="collectionType") String collectionType, 
			HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getSessionStatus(sessionId, contentGooruId));
	}

	@RequestMapping(value="/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/{collectionType}/{collectionId}/users",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_VIEW)
	@ResponseBody
	public ModelAndView getStudentsCollectionUsage(HttpServletRequest request, @PathVariable(value="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId, @PathVariable(value="unitGooruId") String unitGooruId, 
			@PathVariable(value="lessonGooruId") String lessonGooruId, @PathVariable(value="collectionId") String collectionId,
			@PathVariable(value="collectionType") String collectionType,
			HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getStudentsCollectionData(classGooruId, courseGooruId, unitGooruId, lessonGooruId, collectionId, request.isSecure()));
	}
	
	@RequestMapping(value="{collectionType}/{contentGooruId}/user/{userUid}/resources",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getStudentAssessmentSummary(HttpServletRequest request, @RequestParam(value="classGooruId", required = false) String classGooruId,
			@RequestParam(value="courseGooruId", required = false) String courseGooruId, @RequestParam(value="unitGooruId", required = false) String unitGooruId, 
			@RequestParam(value="lessonGooruId", required = false) String lessonGooruId, @PathVariable(value="contentGooruId") String contentGooruId,
			@PathVariable(value="userUid") String userUid, @RequestParam(value="sessionId", required = true) String sessionId, 
			@PathVariable(value="collectionType") String collectionType, HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().getStudentAssessmentSummary(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, userUid, sessionId, request.isSecure()));
	}
	
	@RequestMapping(value="/class/find/usage",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView findIfUsage(HttpServletRequest request, @RequestParam(value="classGooruId", required = false) String classGooruId,
			@RequestParam(value="courseGooruId", required = false) String courseGooruId, @RequestParam(value="unitGooruId", required = false) String unitGooruId, 
			@RequestParam(value="lessonGooruId", required = false) String lessonGooruId, @RequestParam(value="contentGooruId", required = false) String contentGooruId,
			HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getModel(getClassService().findUsageAvailable(classGooruId, courseGooruId,unitGooruId,lessonGooruId,contentGooruId));
	}

	@RequestMapping(value="/class/{classId}/course/{courseId}/unit/{unitId}/lesson/{lessonId}/{collectionType}/{collectionId}/users/report", method = {RequestMethod.GET})
	public void exportClassUserUsageReport(HttpServletRequest request,
			@PathVariable(value ="classId") String classId,@PathVariable(value ="courseId") String courseId,
			@PathVariable(value ="unitId") String unitId,@PathVariable(value ="lessonId") String lessonId,
			@PathVariable(value ="collectionType") String collectionType,@PathVariable(value ="collectionId") String collectionId,HttpServletResponse response) throws JSONException, ParseException, IOException{
		
		File file = getClassExporterProcessor().exportClassUserUsageReport(classId, courseId, unitId, lessonId, collectionType, collectionId);
		generateCSVOutput(response,file);
	}

	@RequestMapping(value="/class/{collectionId}/resources/report", method = {RequestMethod.GET})
	public void exportClassUserUsageReport(HttpServletRequest request,
			@PathVariable(value ="collectionId") String collectionId,
			@RequestParam(value="sessionId") String sessionId,HttpServletResponse response) throws JSONException, ParseException, IOException{
		
		File file = getClassExporterProcessor().exportClassSummaryReport(collectionId,sessionId);
		generateCSVOutput(response,file);
	}
	
	public ClassExporterProcessor getClassExporterProcessor() {
		return classExporterProcessor;
	}

	public void setClassExporterProcessor(
			ClassExporterProcessor classExporterProcessor) {
		this.classExporterProcessor = classExporterProcessor;
	}
}
	
