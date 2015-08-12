package org.gooru.insights.api.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.services.LiveDashboardService;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value="livedashboard/")

public class LiveDashboardController extends BaseController {

	@Autowired
	private LiveDashboardService liveDashboardService;
	
	@RequestMapping(value = "/add/settings", method = { RequestMethod.POST })
	public ModelAndView addSettings(HttpServletRequest request, @RequestParam(value = "cfName", required = true) String cfName, @RequestParam(value = "keyName", required = true) String keyName,
			@RequestParam(value = "data", required = true) String data, HttpServletResponse response) throws Exception {
		
		response = setAllowOrigin(response);
		return getModel(getLiveDashboardService().addSettings(cfName, keyName, data));
	}
        
	@RequestMapping(value = "/add/counter/settings", method = { RequestMethod.POST })
	public ModelAndView addCounterSettings(HttpServletRequest request, @RequestParam(value = "cfName", required = true) String cfName,
			@RequestParam(value = "keyName", required = true) String keyName, @RequestParam(value = "data", required = true) String data, HttpServletResponse response) throws Exception {
		
		response = setAllowOrigin(response);
		return getModel(getLiveDashboardService().addCounterSettings(cfName, keyName, data));
	}
    
	@RequestMapping(value = "/view/settings", method = { RequestMethod.GET })
	public ModelAndView viewSettings(HttpServletRequest request, @RequestParam(value = "cfName", required = true) String cfName, @RequestParam(value = "keyName", required = true) String keyName,
			HttpServletResponse response) throws Exception {

		response = setAllowOrigin(response);
		return getModel(getLiveDashboardService().viewSettings(cfName, keyName));
	}
    
    private LiveDashboardService getLiveDashboardService() {
		return liveDashboardService;
	}
	
	
}
