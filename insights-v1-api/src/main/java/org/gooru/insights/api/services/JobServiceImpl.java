package org.gooru.insights.api.services;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.gooru.insights.api.constants.ErrorMessages;
import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.models.JobStatus;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.models.InsightsConstant.ColumnFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.netflix.astyanax.model.ColumnList;

public class JobServiceImpl implements JobService {

	@Autowired
	private CassandraService cassandraService;
	
	public CassandraService getCassandraService() {
		return cassandraService;
	}
	
	Map<String,String> monitoringKeys = new HashMap<String, String>();
	
	SimpleDateFormat minFormatter = new SimpleDateFormat("yyyyMMddkkmm");
	
	private static final Logger logger = LoggerFactory.getLogger(LiveDashboardServiceImpl.class);
	
	@PostConstruct
	public void init(){
		ColumnList<String> monitorKeyList = cassandraService.getConfigKeys(InsightsOperationConstants.MONITOR_JOBS);
		for(int i = 0 ; i < monitorKeyList.size() ; i++) {
			monitoringKeys.put(monitorKeyList.getColumnByIndex(i).getName(), monitorKeyList.getColumnByIndex(i).getStringValue());
		}
	}
	
	@SuppressWarnings("unused")
	public ResponseParamDTO<Map<String, Object>> getJobStatus(String traceId,ResponseParamDTO<Map<String,Object>> responseParamDTO,JobStatus jobStatus) {
		List<Map<String,Object>> jobList = null;
		Map<String,Object> errorMap = new HashMap<String,Object>();
		if(jobStatus != null) {
			if (jobStatus.getQueue().equalsIgnoreCase(InsightsOperationConstants._ALL)) {
				for (String key : monitoringKeys.keySet()) {
					Map<String,Object> mapData = new HashMap<String,Object>();
					mapData.put("queue", key);
					mapData.put("lagInSeconds", getSingleJobStatus(traceId,key));
					jobList.add(mapData);
				}
			} else if(!jobStatus.getQueue().equalsIgnoreCase(InsightsOperationConstants._ALL)){
				for(String queueName : jobStatus.getQueue().split(",")){
					if(monitoringKeys.containsKey(queueName)) {
						Map<String,Object> mapData = new HashMap<String,Object>();
						mapData.put("queue", queueName);
						mapData.put("lagInSeconds", getSingleJobStatus(traceId,queueName));
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
	
	private String getSingleJobStatus(String traceId,String queue) {
		String lagTime = null;
		try {
			ColumnList<String> settingsMap = cassandraService.read(traceId, ColumnFamily.CONFIG_SETTING.getColumnFamily(), monitoringKeys.get(queue)).getResult();
			Date currentTime = new Date();
			Date lastRunTime = minFormatter.parse(settingsMap.getColumnByName(InsightsOperationConstants.CONSTANT_VALUE).getStringValue());
			long lagInMilliSecs = (currentTime.getTime() - lastRunTime.getTime());
			lagTime = String.format("%02d", TimeUnit.MILLISECONDS.toSeconds(lagInMilliSecs));
		} catch (Exception e2) {
			logger.error("Exception : "+e2);
			throw new InternalError(e2.getMessage());
		}
		return lagTime;
	}
	
}
