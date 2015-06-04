package org.gooru.insights.api.controllers;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.services.ClasspageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value="classpage/")
public class ClasspageRestController extends BaseController
{
	@Autowired
	private ClasspageService classpageService;
	
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
   
}
