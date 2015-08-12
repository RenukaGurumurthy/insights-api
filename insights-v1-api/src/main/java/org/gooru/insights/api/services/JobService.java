package org.gooru.insights.api.services;

import java.text.ParseException;
import java.util.Map;

import org.gooru.insights.api.models.JobStatus;
import org.gooru.insights.api.models.ResponseParamDTO;

public interface JobService {

	ResponseParamDTO<Map<String, Object>> getJobStatus(ResponseParamDTO<Map<String, Object>> responseParamDTO,JobStatus jobStatus);
	
	ResponseParamDTO<Map<String, Object>> getJobMonitorStatus(ResponseParamDTO<Map<String, Object>> responseParamDTO,JobStatus jobStatus) throws ParseException;
	
}
