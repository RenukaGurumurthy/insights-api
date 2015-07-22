package org.gooru.insights.api.services;

import java.util.Map;

import org.gooru.insights.api.models.JobStatus;
import org.gooru.insights.api.models.ResponseParamDTO;

public interface JobService {

	ResponseParamDTO<Map<String, Object>> getJobStatus(String traceId,ResponseParamDTO<Map<String, Object>> responseParamDTO,JobStatus jobStatus);
	
}
