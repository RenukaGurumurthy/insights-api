package org.gooru.insights.api.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.services.ItemService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value="/items")
public class ItemController extends BaseController{
	
	@Autowired
	private ItemService itemService;
	
	@RequestMapping(value="/{itemId}",method ={ RequestMethod.GET})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getItemDetail(HttpServletRequest request,@RequestParam(value="fields",required=true) String fields,@PathVariable(value="itemId") String itemIds,@RequestParam(value="startDate",required = false)String startDate,
			@RequestParam(value="endDate",required = false)String endDate,@RequestParam(value="format",required = false)String format,@RequestParam(value="dateLevel",required = false)String dateLevel,@RequestParam(value="granularity",required = false,defaultValue ="all")String granularity,HttpServletResponse response) throws Exception{
		return getModel(getItemService().getItemDetail(fields, itemIds, startDate, endDate, format, dateLevel,granularity));
	}
	
	@RequestMapping(value="/detail",method ={ RequestMethod.GET})
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getItemDetails(HttpServletRequest request,@RequestParam(value="fields",required=true) String fields,@RequestParam(value="itemId",required=false) String itemIds,@RequestParam(value="startDate",required = false)String startDate,
			@RequestParam(value="endDate",required = false)String endDate,@RequestParam(value="format",required = false)String format,@RequestParam(value="dateLevel",required = false)String dateLevel,@RequestParam(value="granularity",required = false,defaultValue ="all")String granularity,HttpServletResponse response) throws Exception{
		return getModel(getItemService().getItemDetail(fields, itemIds, startDate, endDate, format,dateLevel,granularity));
	}

	@RequestMapping(value="/metadata", method = RequestMethod.GET)
	@AuthorizeOperations(operations =  InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS_VIEWS)
	@ResponseBody
	public ModelAndView getMetadataList(HttpServletRequest request,HttpServletResponse response) throws Exception {
		return getModel(getItemService().getMetadataDetails());
	}
	
	private ItemService getItemService() {
		return itemService;
	}
}
