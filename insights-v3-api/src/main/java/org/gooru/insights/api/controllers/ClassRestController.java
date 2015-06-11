package org.gooru.insights.api.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.models.RequestParamDTO;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.serializer.JsonDeserializer;
import org.gooru.insights.api.services.ClassService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value="collection/")
public class ClassRestController extends BaseController
{
	@Autowired
	private ClassService classService;
   
	@RequestMapping(value="/{collectionGooruId}/sessions",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_OE_VIEW)
	@ResponseBody
	public ModelAndView getSessions(HttpServletRequest request,@RequestBody String data,@PathVariable(value="collectionGooruId") String collectionGooruId,HttpServletResponse response) throws Exception{
		RequestParamDTO requestParam = buildSessionActivityFromInputParameters(data);
		requestParam.setCollectionGooruId(collectionGooruId);
		return getModel(classService.getSessions(requestParam));
	}
	
	@RequestMapping(value="/{collectionGooruId}",method ={RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_COLLECTION_VIEW)
	@ResponseBody
	public ModelAndView getCollection(HttpServletRequest request,@RequestBody String data,@PathVariable(value="collectionGooruId") String collectionGooruId,HttpServletResponse response) throws Exception{
		RequestParamDTO requestParam = buildSessionActivityFromInputParameters(data);
		requestParam.setCollectionGooruId(collectionGooruId);
		return getModel(classService.getCollectionSessionData(requestParam));
	}
	
	@RequestMapping(value="/{collectionGooruId}/resources",method ={RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_RESOURCE_VIEW)
	@ResponseBody
	public ModelAndView getResources(HttpServletRequest request,@RequestBody String data,@PathVariable(value="collectionGooruId") String collectionGooruId,HttpServletResponse response) throws Exception{
		RequestParamDTO requestParam = buildSessionActivityFromInputParameters(data);
		requestParam.setCollectionGooruId(collectionGooruId);
		return getModel(classService.getCollectionResourceSessionData(requestParam));
	}
	
	@RequestMapping(value="/{collectionGooruId}/OE",method ={RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_RESOURCE_VIEW)
	@ResponseBody
	public ModelAndView getOEResponse(HttpServletRequest request,@RequestBody String data,@PathVariable(value="collectionGooruId") String collectionGooruId,HttpServletResponse response) throws Exception{
		RequestParamDTO requestParam = buildSessionActivityFromInputParameters(data);
		requestParam.setCollectionGooruId(collectionGooruId);
		return getModel(classService.getOEResponseData(requestParam));
	}
	
	@RequestMapping(value="/{classGooruId}/report",method ={RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_RESOURCE_VIEW)
	@ResponseBody
	public ModelAndView getMasteryReport(HttpServletRequest request,@RequestBody String data,@PathVariable(value="classGooruId") String classGooruId,HttpServletResponse response) throws Exception{
		RequestParamDTO requestParam = buildSessionActivityFromInputParameters(data);
		requestParam.setClassGooruId(classGooruId);
		return getModel(classService.getMasteryReportDataForFirstSession(requestParam));
	}
	private RequestParamDTO buildSessionActivityFromInputParameters(String data) {
		return JsonDeserializer.deserialize(data, RequestParamDTO.class);
	}
}
