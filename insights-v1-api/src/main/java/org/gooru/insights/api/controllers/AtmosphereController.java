package org.gooru.insights.api.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.atmosphere.cpr.AtmosphereRequest;
import org.atmosphere.cpr.AtmosphereResource;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.services.BaseService;
import org.gooru.insights.api.services.ClassPageService;
import org.gooru.insights.api.services.LiveDashboardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class AtmosphereController extends AtmosphereResolver {

	@Autowired
	private LiveDashboardService liveDashboardService;

	@Autowired
	private ClassPageService classPageService;

	private BaseService baseService;

	private ArrayList<HttpSession> subscribers = new ArrayList<HttpSession>();

	private ArrayList<HttpSession> progressSubcriber = new ArrayList<HttpSession>();

	// Dashboard activity GET
	@RequestMapping(value = "/push/message", method = RequestMethod.GET)
	@ResponseBody
	public void onRequest(AtmosphereResource atmosphereResource, HttpSession session, HttpServletResponse response) throws IOException {

		response = addCrossDomainSupport(response);
		AtmosphereRequest atmosphereRequest = atmosphereResource.getRequest();

		if (atmosphereRequest.getHeader("negotiating") == null) {
			atmosphereResource.resumeOnBroadcast(atmosphereResource.transport() == AtmosphereResource.TRANSPORT.LONG_POLLING).suspend();
		} else {
			atmosphereResource.getResponse().getWriter().write("OK");
		}
		for (HttpSession httpSession : subscribers) {
			subscribers.add(session);
		}

	}

	// Dashboard activity POST
	@RequestMapping(value = "/push/message", method = RequestMethod.POST)
	@ResponseBody
	public void onPost(AtmosphereResource atmosphereResource, HttpServletResponse response) throws Exception {

		// add cross domain support
		response.setContentType("text/html,application/xhtml+xml,application/xml;charset=UTF-8");
		response = addCrossDomainSupport(response);
		AtmosphereRequest atmosphereRequest = atmosphereResource.getRequest();
		String data = atmosphereRequest.getParameter("data");
		/**
		 * Depricated
		 * List<Map<String, Object>> finalData = getLiveDashboardService().getLiveData(data);
		
		 */
		String resultSet = baseService.listMapToJsonString(new ArrayList<Map<String,Object>>());
		atmosphereResource.getBroadcaster().broadcast(resultSet);

	}

	// Teacher's progress page GET
	@RequestMapping(value = "/classpage/{classId}/collection/{collectionId}/users/usage", method = RequestMethod.GET)
	@ResponseBody
	public void onProgessRequest(AtmosphereResource atmosphereProgress, @PathVariable(value = "collectionId") String collectionId, @PathVariable(value = "classId") String classId,
			HttpSession progressSession, HttpServletResponse response) throws IOException {

		response = addCrossDomainSupport(response);
		AtmosphereRequest atmosphereProgressRequest = atmosphereProgress.getRequest();

		if (atmosphereProgressRequest.getHeader("negotiating") == null) {
			atmosphereProgress.resumeOnBroadcast(atmosphereProgress.transport() == AtmosphereResource.TRANSPORT.LONG_POLLING).suspend();
		} else {
			atmosphereProgress.getResponse().getWriter().write("OK");
		}
		for (HttpSession pSession : progressSubcriber) {
			progressSubcriber.add(progressSession);
		}

	}

	// Teacher's progress page POST
	@RequestMapping(value = "/classpage/{classId}/collection/{collectionId}/users/usage", method = RequestMethod.POST)
	@ResponseBody
	public void onProgessPost(HttpServletRequest request, AtmosphereResource atmosphereProgress, @PathVariable(value = "collectionId") String collectionId, @PathVariable(value = "classId") String classId,
			HttpServletResponse response) throws Exception {

		response = addCrossDomainSupport(response);
		AtmosphereRequest atmosphereProgressRequest = atmosphereProgress.getRequest();
		String data = atmosphereProgressRequest.getParameter("data");
		ResponseParamDTO<Map<String, Object>> resultSet = getClassPageService().getClasspageUserUsage(collectionId, data,request.isSecure());
		atmosphereProgress.getBroadcaster().broadcast(resultSet);

	}

	private HttpServletResponse addCrossDomainSupport(HttpServletResponse response) {
		// add cross domain support
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST");
		return response;
	}

	private LiveDashboardService getLiveDashboardService() {
		return liveDashboardService;
	}

	private ClassPageService getClassPageService() {
		return classPageService;
	}

}
