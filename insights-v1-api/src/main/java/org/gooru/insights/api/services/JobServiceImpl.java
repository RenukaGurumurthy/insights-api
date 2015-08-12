package org.gooru.insights.api.services;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.models.InsightsConstant.ColumnFamily;
import org.gooru.insights.api.models.JobStatus;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.utils.DataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

@Service
public class JobServiceImpl implements JobService {

	@Autowired
	private CassandraService cassandraService;
	
	public CassandraService getCassandraService() {
		return cassandraService;
	}
	
	Map<String,String> monitoringKeys = new HashMap<String, String>();
	
	SimpleDateFormat minFormatter = new SimpleDateFormat("yyyyMMddkkmm");
	
	SimpleDateFormat dateToMinFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static final Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);
	
	@PostConstruct
	public void init(){
		ColumnList<String> monitorKeyList = cassandraService.getConfigKeys(InsightsOperationConstants.MONITOR_JOBS);
		if (monitorKeyList != null && !monitorKeyList.isEmpty()) {
			for (int i = 0; i < monitorKeyList.size(); i++) {
				monitoringKeys.put(monitorKeyList.getColumnByIndex(i).getName(), monitorKeyList.getColumnByIndex(i).getStringValue());
			}
		} else {
			logger.debug("Monitoring columns are unavailable in job_config_settings columnfamily for key :" + InsightsOperationConstants.MONITOR_JOBS);
		}
	}
	
	@SuppressWarnings("unused")
	public ResponseParamDTO<Map<String, Object>> getJobStatus(ResponseParamDTO<Map<String,Object>> responseParamDTO,JobStatus jobStatus) {
		List<Map<String,Object>> jobList = new ArrayList<Map<String,Object>>();
		Map<String,Object> errorMap = new HashMap<String,Object>();
		if(jobStatus != null) {
			if (jobStatus.getQueue().equalsIgnoreCase(InsightsOperationConstants._ALL)) {
				for (String key : monitoringKeys.keySet()) {
					Map<String,Object> mapData = new HashMap<String,Object>();
					mapData.put("queue", key);
					mapData.put("lagInSeconds", getSingleJobStatus(key));
					jobList.add(mapData);
				}
			} else if(!jobStatus.getQueue().equalsIgnoreCase(InsightsOperationConstants._ALL)){
				for(String queueName : jobStatus.getQueue().split(",")){
					if(monitoringKeys.containsKey(queueName)) {
						Map<String,Object> mapData = new HashMap<String,Object>();
						mapData.put("queue", queueName);
						mapData.put("lagInSeconds", getSingleJobStatus(queueName));
						jobList.add(mapData);
					} else if(!monitoringKeys.containsKey(queueName)) {
						errorMap.put(queueName, ErrorMessages.E113);
					}
				}
			}
			if(jobList != null && jobList.size() > 0) {
				responseParamDTO.setContent(jobList);
			} 
		} else {
			errorMap.put("error", "Fields must not be empty");
		}
		if(!errorMap.isEmpty()) {
			responseParamDTO.setMessage(errorMap);
		}
		return responseParamDTO;
	}
	
	private String getSingleJobStatus(String queue) {
		String lagTime = null;
		try {
			ColumnList<String> settingsMap = cassandraService.read(ColumnFamily.CONFIG_SETTING.getColumnFamily(), monitoringKeys.get(queue)).getResult();
			Date lastRunTime = minFormatter.parse(settingsMap.getColumnByName(InsightsOperationConstants.CONSTANT_VALUE).getStringValue());
			lagTime = DataUtils.getTimeDifference(lastRunTime);
		} catch (Exception e2) {
			logger.error("Exception : "+e2);
			throw new InternalError(e2.getMessage());
		}
		return lagTime;
	}

	@Override
	public ResponseParamDTO<Map<String, Object>> getJobMonitorStatus(ResponseParamDTO<Map<String, Object>> responseParamDTO,JobStatus jobStatus) {
		List<Map<String,Object>> jobList = new ArrayList<Map<String,Object>>();
		Map<String,Object> errorMap = new HashMap<String,Object>();
		if(jobStatus != null) {
			Rows<String, String> runningJobs = getCassandraService().read(ColumnFamily.JOB_TRACKER.getColumnFamily(), "running_status", 1).getResult();
			Map<String,String> jobDetails = new HashMap<String,String>();
			for(int i=0;i<runningJobs.size();i++) {
				jobDetails.put(runningJobs.getRowByIndex(i).getKey(), runningJobs.getRowByIndex(i).getColumns().getColumnByName("modified_on").getStringValue());
			}
			
			
			if(jobStatus.getQueue().equalsIgnoreCase(InsightsOperationConstants._ALL)) {
				for(Map.Entry<String,String> data : jobDetails.entrySet()) {
					Map<String,Object> mapData = new HashMap<String,Object>();
					mapData.put("jobName", data.getKey());
					mapData.put("lagInSeconds", getMonitorJobStatus(data.getValue()));
					jobList.add(mapData);
				}
			} else if(!jobStatus.getQueue().equalsIgnoreCase(InsightsOperationConstants._ALL)) {
				for(String queueName : jobStatus.getQueue().split(",")){
					if(jobDetails.containsKey(queueName)) {
						Map<String,Object> mapData = new HashMap<String,Object>();						
						mapData.put("jobName", queueName);
						mapData.put("lagInSeconds", getMonitorJobStatus(jobDetails.get(queueName)));
						jobList.add(mapData);
					} else if(!jobDetails.containsKey(queueName)) {
						errorMap.put(queueName, ErrorMessages.E113);
					}
				}
			}
			if(jobList != null && jobList.size() > 0) {
				responseParamDTO.setContent(jobList);
			} 
		} else {
			errorMap.put("error", "Fields must not be empty");
		}
		if(!errorMap.isEmpty()) {
			responseParamDTO.setMessage(errorMap);
		}
		return responseParamDTO;
	}	
	
	public String getMonitorJobStatus(String lastModified) {
		String lagTime = null;
		try {
			Date lastRunTime = dateToMinFormatter.parse(lastModified);
			lagTime = DataUtils.getTimeDifference(lastRunTime);
		} catch (Exception e2) {
			logger.error("Exception : "+e2);
			throw new InternalError(e2.getMessage());
		}
		return lagTime;

	}
	
}
