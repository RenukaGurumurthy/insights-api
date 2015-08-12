package org.gooru.insights.api.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.services.ConfigurationService;
import org.gooru.insights.api.utils.RequestUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value = "/configuration")
public class ConfigurationController extends BaseController {

	@Autowired
	private ConfigurationService configurationService;

	@RequestMapping(value = "/formula", method = RequestMethod.PUT)
	@AuthorizeOperations(operations = InsightsOperationConstants.OPERATION_INSIGHTS_CONFIG_SETTINGS_ADD)
	@ResponseBody
	public ModelAndView addFormula(HttpServletRequest request,
			@RequestParam(value = "eventName", required = true) String eventName,
			@RequestParam(value = "formula", required = true) JSONObject formulaJSON,@RequestParam(value = "aggregateType", required = false) String aggregateType, HttpServletResponse response) throws Exception {
		return getModel(configurationService.addFormula(RequestUtils.getSessionToken(request),eventName,aggregateType, formulaJSON));
	}

	@RequestMapping(value = "/formula", method = RequestMethod.GET)
	@AuthorizeOperations(operations = InsightsOperationConstants.OPERATION_INSIGHTS_CONFIG_SETTINGS_VIEW)
	@ResponseBody
	public ModelAndView listFormula(HttpServletRequest request,
			@RequestParam(value = "eventName", required = true) String eventName, HttpServletResponse response) throws JSONException {
		return getModel(configurationService.listFormula(RequestUtils.getSessionToken(request),eventName));
	}
	
	@RequestMapping(value="/add/settings",method={RequestMethod.POST})
    public ModelAndView addSettings(HttpServletRequest request,@RequestParam(value="cfName",required = false) String cfName,@RequestParam(value="keyName",required = true) String keyName,@RequestParam(value="data",required = true) String data,HttpServletResponse response) throws Exception {
		
            return getModel(configurationService.addSettings(cfName, keyName, data));
    }
        
    @RequestMapping(value="/add/counter/settings",method={RequestMethod.POST})
    public ModelAndView addCounterSettings(HttpServletRequest request,@RequestParam(value="cfName",required=false) String cfName,@RequestParam(value="keyName",required=true) String keyName,@RequestParam(value="data",required=true) String data,HttpServletResponse response) throws Exception {

        return getModel(configurationService.addCounterSettings(cfName,keyName,data));
    }
    
    @RequestMapping(value="/view/settings",method={RequestMethod.GET})
    public ModelAndView viewSettings(HttpServletRequest request,@RequestParam(value="cfName",required = false) String cfName,@RequestParam(value="keyName",required = true) String keyName,HttpServletResponse response) throws Exception {
    		
    	 return getModel(configurationService.viewSettings(cfName, keyName));
    }
    
    @RequestMapping(value="migrate/row",method={RequestMethod.POST})
	public ModelAndView migrateData(HttpServletRequest request,@RequestParam(value="sourceCF",required=true) String sourceCF,
			@RequestParam(value="targetCF",required=true) String targetCF,@RequestParam(value="sourceKey",required=true) String sourceKey,@RequestParam(value="targetKey",required=false) String targetKey,HttpServletResponse response) throws Exception {
    	
    	return getModel(configurationService.migrateCFData(sourceCF,targetCF,sourceKey,targetKey));
	}
	
}
