package org.gooru.insights.api.controllers;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.models.JobStatus;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.services.JobService;
import org.gooru.insights.api.spring.exception.BadRequestException;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

public class JobController extends BaseController {

	@Autowired
	private JobService jobService;
	
    public JobService getJobService() {
		return jobService;
	}

	@RequestMapping(value="/queue-status/{jobType}",method={RequestMethod.GET})
	public ModelAndView getJobStatus(HttpServletRequest request, @PathVariable(value = "jobType") String jobType, @RequestParam(value = "format", required = false,defaultValue = "json") String format,
 HttpServletResponse response) throws JSONException {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();

		JobStatus jobStatus = new JobStatus();
		jobStatus.setQueue(jobType);

		if (format.equalsIgnoreCase(InsightsOperationConstants.SIMPLE_TEXT) && (jobType.equalsIgnoreCase(InsightsOperationConstants._ALL) || jobType.split(",").length > 1)) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			throw new BadRequestException(ErrorMessages.E112);
		}
	
		responseParamDTO = getJobService().getJobStatus(getTraceId(request),responseParamDTO, jobStatus);

		if (responseParamDTO != null && !responseParamDTO.getMessage().isEmpty()) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			throw new BadRequestException(String.valueOf(responseParamDTO.getMessage().get("error")));
		}
		if (format.equalsIgnoreCase(InsightsOperationConstants.SIMPLE_TEXT)) {
			return getSimpleModel(String.valueOf(responseParamDTO.getContent().get(0).get("lagInSeconds")));
		} else {
			return getModel(responseParamDTO);
		}
	}
	
}
