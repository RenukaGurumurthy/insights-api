package org.gooru.insights.api.controllers;

import java.util.HashMap;
import java.util.Map;

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
	private ClassService classpageService;
	
	@RequestMapping(value="/title/{collectionId}",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_OE_VIEW)
	@ResponseBody
	public ModelAndView getTestContentTitle(HttpServletRequest request,@PathVariable(value="collectionId") Integer collectionId,HttpServletResponse response) throws Exception{
		Map<String, Object> m = new HashMap<String, Object>();
		String title = classpageService.getTitle(collectionId);
		m.put("collectionId", collectionId);
		m.put("collectionTitle", title);
		return getModel(m);
	}
   
	@RequestMapping(value="/{collectionGooruId}/sessions",method ={ RequestMethod.GET,RequestMethod.POST})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_CLASS_OE_VIEW)
	@ResponseBody
	public ModelAndView getSessions(HttpServletRequest request,@RequestBody String data,@PathVariable(value="collectionGooruId") String collectionGooruId,HttpServletResponse response) throws Exception{
		Map<String, Object> m = new HashMap<String, Object>();
		RequestParamDTO requestParam = buildSessionActivityFromInputParameters(data);
		m.put("collectionGooruId", collectionGooruId);
		m.put("classGooruId", requestParam.getClassGooruId());
		m.put("reportType", requestParam.getReportType());
		return getModel(m);
	}
	
	private RequestParamDTO buildSessionActivityFromInputParameters(String data) {
		return JsonDeserializer.deserialize(data, RequestParamDTO.class);
	}
}
