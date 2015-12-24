package org.gooru.insights.api.controllers;

import java.io.File;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.services.ClassPageService;
import org.gooru.insights.api.utils.RequestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value="classpage/")
public class ClassPageController extends BaseController{

	@Autowired
	private ClassPageService classPageService;
	
	@RequestMapping(value="/{collectionId}/OEText",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_OE_VIEW)
	@ResponseBody
	public ModelAndView getClasspageResourceOE(HttpServletRequest request,@PathVariable(value="collectionId") String collectionId,@RequestParam(value="data",required = false)String data,@RequestBody String rawData,HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		data = RequestUtils.getRequestData(request,rawData);
		return getModel(getClassPageService().getClasspageResourceOEtext(collectionId,data));
	}
	
	@RequestMapping(value="/{collectionId}",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_COLLECTION_VIEW)
	@ResponseBody
	public ModelAndView getClasspageCollectionUsage(HttpServletRequest request,@PathVariable(value="collectionId") String collectionId,@RequestParam(value="data",required = false)String data, @RequestBody String rawData, HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		data = RequestUtils.getRequestData(request,rawData);
		return getModel(getClassPageService().getClasspageCollectionUsage(collectionId,data,request.isSecure()));
		
	}

	@RequestMapping(value="/{collectionId}/resources",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_RESOURCE_VIEW)
	@ResponseBody
	public ModelAndView getClasspageResourceUsage(HttpServletRequest request,@PathVariable(value="collectionId") String collectionId,@RequestParam(value="data",required = false)String data,@RequestBody String rawData,HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		data = RequestUtils.getRequestData(request,rawData);
		return getModel(getClassPageService().getClasspageResourceUsage(collectionId,data,request.isSecure()));
	}
	
	//teacher
	@RequestMapping(value="/{classId}/users",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_PROGRESS_VIEW)
	@ResponseBody
	public ModelAndView getClasspageUserList(HttpServletRequest request,@PathVariable(value="classId") String classId,@RequestParam(value="data",required = false)String data,@RequestBody String rawData,HttpServletResponse response) throws Exception{
		data = RequestUtils.getRequestData(request,rawData);
		return getModel(getClassPageService().getClasspageUsers(classId,data));
	}
	
	//teacher
	@RequestMapping(value="/{collectionId}/users/usage",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_PROGRESS_VIEW)
	@ResponseBody
	public ModelAndView getClasspageUserUsage(HttpServletRequest request,@PathVariable(value="collectionId") String collectionId,@RequestParam(value="data",required = false)String data,@RequestBody String rawData,HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		data = RequestUtils.getRequestData(request,rawData);
		return getModel(getClassPageService().getClasspageUserUsage(collectionId,data,request.isSecure()));
	}
	
	@RequestMapping(value="/{collectionId}/sessions",method={RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_COLLECTION_VIEW)
	@ResponseBody
	public ModelAndView getUserSessions(HttpServletRequest request,@PathVariable(value="collectionId") String collectionId,@RequestParam(value="data",required = false) String data,@RequestBody String rawData,HttpServletResponse response) throws Exception {			
		setAllowOrigin(response);
		data = RequestUtils.getRequestData(request,rawData);
		return getModel(getClassPageService().getUserSessions(data,collectionId));
	}
	
	//teacher
	@RequestMapping(value="/{classId}/grade",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_PROGRESS_VIEW)
	@ResponseBody
	public ModelAndView getClassGrade(HttpServletRequest request,@PathVariable(value="classId") String classId,@RequestParam(value="data",required = false)String data,@RequestBody String rawData,HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		data = RequestUtils.getRequestData(request,rawData);
		return getModel(getClassPageService().getClasspageGrade(classId,data,request.isSecure()));
	}
	
	@RequestMapping(value="/{resouceId}/resource/info",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_PROGRESS_VIEW)
	@ResponseBody
	public ModelAndView getResourceInfo(HttpServletRequest request,@PathVariable(value="resouceId") String resouceId,@RequestParam(value="data",required = false)String data,@RequestBody String rawData,HttpServletResponse response) throws Exception{
		data = RequestUtils.getRequestData(request,rawData);
		return getModel(getClassPageService().getResourceInfo(resouceId,data));
	}
	
	@RequestMapping(value="/{classId}/{reportType}/export",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_PROGRESS_VIEW)
	@ResponseBody
	public ModelAndView getExportReports(HttpServletRequest request,@PathVariable(value="classId") String classId,@PathVariable(value="reportType") String reportType,@RequestParam(value="data",required = false)String data,@RequestParam(value="timeZone",required = true)String timeZone,@RequestBody String rawData,HttpServletResponse response) throws Exception{
		String format = checkRequestContentType(request);
		data = RequestUtils.getRequestData(request,rawData);
		setAllowOrigin(response);
		ResponseParamDTO<Map<String,Object>> responseParamsDTO = getClassPageService().getExportReport(format,classId,reportType,data,timeZone,response);
		if (responseParamsDTO.getContent() != null && !responseParamsDTO.getContent().isEmpty()) {
			if (!format.equalsIgnoreCase("json")) {
				for (Map<String, Object> map : responseParamsDTO.getContent()) {
					File file = new File(map.get("file").toString());
					generateExcelOutput(response, file);
					file.delete();
				}
			}
		} else {
			response.sendError(204,
					"No content for your request provide valid content.");
		}
		return getModel(responseParamsDTO);
	}

	//teacher
	@RequestMapping(value="/{classId}/progress",method ={ RequestMethod.GET,RequestMethod.POST})
	@ResponseBody
	public ModelAndView getClassProgress(HttpServletRequest request,@PathVariable(value="classId") String classId,@RequestParam(value="data",required = false)String data,@RequestBody String rawData,HttpServletResponse response) throws Exception{
		setAllowOrigin(response);
		data = RequestUtils.getRequestData(request,rawData);
		return getModel(getClassPageService().getClassProgress(classId,data));
	}
	
	private ClassPageService getClassPageService() {
		return classPageService;
	}
	
}
