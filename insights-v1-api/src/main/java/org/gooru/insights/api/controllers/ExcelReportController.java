package org.gooru.insights.api.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.security.AuthorizeOperations;
import org.gooru.insights.api.services.ExcelReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping(value = "/reports")
@EnableAsync
public class ExcelReportController extends BaseController implements InsightsConstant {

	@Autowired
	private ExcelReportService excelReportService;
	
	@RequestMapping(value = "/activity/dump", method = RequestMethod.POST)
	@ResponseBody
	@AuthorizeOperations(operations = InsightsOperationConstants.OPERATION_INSIHGHTS_REPORTS)
	public ModelAndView getActitivity(HttpServletRequest request, @RequestParam(value = "email", required = false) String emailId, @RequestParam(value = "data", required = true) String data,
			HttpServletResponse response) throws Exception {
		
		String format = checkRequestContentType(request);
		return getModel(getExcelReport().getPerformDump(data, format, emailId));
	}


	/**
	 * TODO schedular to remove file
	 */
	public void removeExpiredFiles(){
		excelReportService.removeExpiredFiles();
	}

	private ExcelReportService getExcelReport() {

		return excelReportService;
	}
}
