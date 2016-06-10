package org.gooru.insights.api.controllers;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.models.ContentTaxonomyActivity;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.models.SessionTaxonomyActivity;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.services.ClassService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

@RestController
@RequestMapping(value="/v2/")
public class ClassController extends BaseController {

	@Autowired
	private ClassService classService;

	private ClassService getClassService() {
		return classService;
	}

	@RequestMapping(value = "/class/{classGooruId}/user/{userUid}/current/location", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getUserCurrentLocationInLesson(HttpServletRequest request,
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="userUid") String userUid,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getUserCurrentLocationInLesson(userUid, classGooruId));
	}

	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/peers", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getCoursePeers(HttpServletRequest request,
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getUserPeers(classGooruId, courseGooruId, null, ApiConstants.UNIT));
	}

	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/peers", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getUnitPeers(HttpServletRequest request,
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId,
			@PathVariable(value="unitGooruId") String unitGooruId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getUserPeers(classGooruId, courseGooruId, unitGooruId, ApiConstants.LESSON));
	}

	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/peers", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getLessonPeers(HttpServletRequest request,
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value="courseGooruId") String courseGooruId,
			@PathVariable(value="unitGooruId") String unitGooruId,
			@PathVariable(value="lessonGooruId") String lessonGooruId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(
			classService.getUserPeers(classGooruId, courseGooruId, unitGooruId, lessonGooruId, ApiConstants.CONTENT));
	}

	@RequestMapping(value = "{collectionType}/{contentGooruId}/session/{sessionId}/status", method = {RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getSessionStatus(HttpServletRequest request,
			@PathVariable(value = "sessionId") String sessionId,
			@PathVariable(value = "contentGooruId") String contentGooruId,
			@PathVariable(value = "collectionType") String collectionType, HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getSessionStatus(sessionId, contentGooruId));
	}

	@RequestMapping(value="/{collectionType}/{contentGooruId}/sessions",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getUserSessions(HttpServletRequest request,
			@RequestParam(value="classGooruId", required = false, defaultValue = "NA") String classGooruId,
			@RequestParam(value="courseGooruId", required = false, defaultValue = "NA") String courseGooruId,
			@RequestParam(value="unitGooruId", required = false, defaultValue = "NA") String unitGooruId,
			@RequestParam(value="lessonGooruId", required = false, defaultValue = "NA") String lessonGooruId,
			@PathVariable(value="collectionType") String collectionType,
			@PathVariable(value="contentGooruId") String contentGooruId,
			@RequestParam(value="userUid", required = true) String userUid,
			@RequestParam(value="openSession", required = false) boolean openSession,
			HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		return getDeferredResult(classService
			.getUserSessions(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, collectionType, userUid, openSession));
	}

	@RequestMapping(value="/{collectionType}/{contentGooruId}/user/{userUid}",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getSummaryData(HttpServletRequest request,
			@PathVariable(value="collectionType") String collectionType, @PathVariable(value="userUid") String userUid,
			@RequestParam(value="sessionId", required = false,defaultValue = "NA") String sessionId, @RequestParam(value="classGooruId", required = false,defaultValue = "NA") String classGooruId,
			@RequestParam(value="courseGooruId", required = false,defaultValue = "NA") String courseGooruId, @RequestParam(value="unitGooruId", required = false,defaultValue = "NA") String unitGooruId,
			@RequestParam(value="lessonGooruId", required = false,defaultValue = "NA") String lessonGooruId, @PathVariable(value="contentGooruId") String contentGooruId,
			HttpServletResponse response) throws Exception {
		setAllowOrigin(response);
		return getDeferredResult(classService
			.getSummaryData(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, sessionId, userUid, collectionType));
	}

	@RequestMapping(value = "/{collectionType}/{contentGooruId}/user/{userUid}/prior/usage", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getPriorUsage(HttpServletRequest request,
			@PathVariable(value="collectionType") String collectionType, @PathVariable(value="userUid") String userUid,
			@RequestParam(value="sessionId", required = false) String sessionId, @RequestParam(value="classGooruId", required = false) String classGooruId,
			@RequestParam(value="courseGooruId", required = false) String courseGooruId, @RequestParam(value="unitGooruId", required = false) String unitGooruId,
			@RequestParam(value="lessonGooruId", required = false) String lessonGooruId, @PathVariable(value="contentGooruId") String contentGooruId,
			@RequestParam(value="openSession", defaultValue = "true", required = false) boolean openSession,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService
			.getPriorDetail(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, sessionId, userUid,collectionType, openSession));
	}

	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/performance", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getCoursePerformance(HttpServletRequest request,
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value = "courseGooruId") String courseGooruId,
			@RequestParam(value = "userUid", required = false) String userUid,
			@RequestParam(value = "collectionType", required = true) String collectionType,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(
			classService.getPerformance(classGooruId, courseGooruId, null, null, userUid, collectionType, ApiConstants.UNIT));
	}

	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/performance", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getUnitPerformance(HttpServletRequest request,
			@PathVariable(value ="classGooruId") String classGooruId,
			@PathVariable(value = "courseGooruId") String courseGooruId,
			@PathVariable(value = "unitGooruId") String unitGooruId,
			@RequestParam(value = "userUid", required = false) String userUid,
			@RequestParam(value = "collectionType", required = true) String collectionType,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService
			.getPerformance(classGooruId, courseGooruId, unitGooruId, null, userUid, collectionType, ApiConstants.LESSON));
	}

	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/performance", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getLessonPerformance(HttpServletRequest request,
			@PathVariable(value = "classGooruId") String classGooruId,
			@PathVariable(value = "courseGooruId") String courseGooruId,
			@PathVariable(value = "unitGooruId") String unitGooruId,
			@PathVariable(value = "lessonGooruId") String lessonGooruId,
			@RequestParam(value = "userUid", required = false) String userUid,
			@RequestParam(value = "collectionType", required = true) String collectionType,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService
			.getPerformance(classGooruId, courseGooruId, unitGooruId, lessonGooruId, userUid, collectionType, ApiConstants.CONTENT));
	}

	@RequestMapping(value = "/class/{classGooruId}/course/{courseGooruId}/unit/{unitGooruId}/lesson/{lessonGooruId}/{collectionType}/{contentGooruId}/performance", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getAllStudentContentPerformance(HttpServletRequest request,
			@PathVariable(value = "classGooruId") String classGooruId,
			@PathVariable(value = "courseGooruId") String courseGooruId,
			@PathVariable(value = "unitGooruId") String unitGooruId,
			@PathVariable(value = "lessonGooruId") String lessonGooruId,
			@PathVariable(value = "contentGooruId") String contentGooruId,
			@PathVariable(value = "collectionType") String collectionType,
			@RequestParam(value = "userUid", required = false) String userUid,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService
			.getAllStudentPerformance(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, collectionType, userUid));
	}

	@RequestMapping(value = "/user/{userUid}/taxonomy/subject/{subjectId}/mastery", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<ContentTaxonomyActivity>> getUserSubjectMastery(HttpServletRequest request,
			@PathVariable(value = "userUid") String userUid, @PathVariable(value = "subjectId") String subjectId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getTaxonomyActivity(1, userUid, subjectId));
	}

	@RequestMapping(value = "/user/{userUid}/taxonomy/subject/{subjectId}/course/{courseId}/mastery", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<ContentTaxonomyActivity>> getUserCourseMastery(HttpServletRequest request,
			@PathVariable(value = "userUid") String userUid, @PathVariable(value = "subjectId") String subjectId,
			@PathVariable(value = "courseId") String courseId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getTaxonomyActivity(2, userUid, subjectId, courseId));
	}

	@RequestMapping(value = "/user/{userUid}/taxonomy/subject/{subjectId}/course/{courseId}/domain/{domainId}/mastery", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<ContentTaxonomyActivity>> getUserDomainMastery(HttpServletRequest request,
			@PathVariable(value = "userUid") String userUid, @PathVariable(value = "subjectId") String subjectId,
			@PathVariable(value = "courseId") String courseId, @PathVariable(value = "domainId") String domainId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getTaxonomyActivity(3, userUid, subjectId, courseId, domainId));
	}

	@RequestMapping(value = "/user/{userUid}/taxonomy/subject/{subjectId}/course/{courseId}/domain/{domainId}/standards/{standardsId}/mastery", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<ContentTaxonomyActivity>> getUserStandardsMastery(HttpServletRequest request,
			@PathVariable(value = "userUid") String userUid, @PathVariable(value = "subjectId") String subjectId,
			@PathVariable(value = "courseId") String courseId, @PathVariable(value = "domainId") String domainId,
			@PathVariable(value = "standardsId") String standardsId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getTaxonomyActivity(4, userUid, subjectId, courseId, domainId, standardsId));
	}

	@RequestMapping(value = "/user/{userUid}/taxonomy/subject/{subjectId}/courses/domain/{domainId}/mastery", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<ContentTaxonomyActivity>> getUserDomainParentMastery(HttpServletRequest request,
			@PathVariable(value = "userUid") String userUid, @RequestParam(value = "domainIds", required = true) String domainIds,HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getUserDomainParentMastery(userUid, domainIds));
	}

	@RequestMapping(value = "/user/{userUid}/grade", method = { RequestMethod.GET, RequestMethod.POST })
	@AuthorizeOperations(operations = InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getTeacherGrade(HttpServletRequest request,
			@PathVariable(value = "userUid") String userUid,
			@RequestParam(value = "sessionId", required = true) String sessionId,
			@RequestParam(value = "teacherId", required = false) String teacherId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		if(request.getAttributeNames() != null) {
			if(request.getAttributeNames().toString().contains(ApiConstants.GOORU_U_ID))
				teacherId = (String) request.getAttribute(ApiConstants.GOORU_U_ID);
		}
		//TODO Infer teacherUid from sessionToken and save to request attribute when authorizing API Caller, after this logic is added remove teacherId request parameter.

		return getDeferredResult(classService.getTeacherGrade(teacherId, userUid, sessionId));
	}

	@RequestMapping(value = "/user/{userUid}/resource/usage", method = RequestMethod.POST)
	@AuthorizeOperations(operations =InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	public DeferredResult<ResponseParamDTO<SessionTaxonomyActivity>> getResourceUsage(HttpServletRequest request,
			@PathVariable(value = "userUid") String userUid,
			@RequestBody String resourceIds,HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getResourceUsage(userUid, resourceIds));
	}

	@RequestMapping(value="/{collectionType}/stat/metrics",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public DeferredResult<ResponseParamDTO<Map<String, Object>>> getStatisticalMetrics(HttpServletRequest request,
			@PathVariable(value="collectionType") String collectionType,@RequestBody String contentGooruIds,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getStatisticalMetrics(contentGooruIds));
	}

	@RequestMapping(value="/session/{sessionId}/taxonomy/usage",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public DeferredResult<ResponseParamDTO<SessionTaxonomyActivity>> getSessionTaxonomyActivity(HttpServletRequest request,
			@PathVariable(value="sessionId") String sessionId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getSessionTaxonomyActivity(sessionId, ApiConstants.DOMAIN));
	}
	@RequestMapping(value="/activity/{eventId}/info",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public DeferredResult<ResponseParamDTO<Map<String,Object>>> getEventToCheck(HttpServletRequest request,
			@PathVariable(value="eventId") String eventId,
			HttpServletResponse response) {
		setAllowOrigin(response);
		return getDeferredResult(classService.getEvent(eventId));
	}

}
