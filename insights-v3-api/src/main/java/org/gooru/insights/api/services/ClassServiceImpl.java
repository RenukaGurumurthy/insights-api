package org.gooru.insights.api.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ErrorConstants;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.RequestParamDTO;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.models.SessionActivity;
import org.gooru.insights.api.repository.ClassRepository;
import org.gooru.insights.api.utils.ValidationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClassServiceImpl implements ClassService,InsightsConstant {

	@Autowired
	private ClassRepository classRepository;

	@Override
	public ResponseParamDTO<Map<String, Object>> getSessions(RequestParamDTO requestParamDTO) {		
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, Object> responseMessage = null;
		List<Map<String, Object>> sessionDataList = classRepository.getSession(classRepository.getContentId(requestParamDTO.getClassGooruId()), classRepository.getContentId(requestParamDTO.getCollectionGooruId()), requestParamDTO.getUserUid());
		if(sessionDataList != null) {
			responseParamDTO.setContent(sessionDataList);
		} else {
			responseMessage = new HashMap<String, Object>();
			responseMessage.put(ApiConstants.DEVELOPER_MESSAGE, ApiConstants.DEFAULT_NOT_FOUND_MESSAGE);
			responseParamDTO.setMessage(responseMessage);
		}		
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getCollectionSessionData(RequestParamDTO requestParamDTO) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, Object> responseMessage = null;
		List<Map<String, Object>> collectionSessionDataList = null; 
		Long sessionId = requestParamDTO.getSessionId();
		if (StringUtils.isNotBlank(requestParamDTO.getCollectionGooruId()) && StringUtils.isNotBlank(requestParamDTO.getClassGooruId())) {
			if (sessionId == null || StringUtils.isBlank(sessionId.toString()) || sessionId.equals(0L)) {
				collectionSessionDataList = classRepository.getCollectionAggregatedDataByAllSession(classRepository.getContentId(requestParamDTO.getClassGooruId()),
						classRepository.getContentId(requestParamDTO.getCollectionGooruId()));
			} else {
				collectionSessionDataList = classRepository.getCollectionSessionData(classRepository.getContentId(requestParamDTO.getClassGooruId()),
						classRepository.getContentId(requestParamDTO.getCollectionGooruId()), sessionId);
			}
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorConstants.E102, ApiConstants.COLLECTIONGOORUID, ApiConstants.CLASSGOORUID);
		}
		if(collectionSessionDataList != null) {
			responseParamDTO.setContent(collectionSessionDataList);
		} else {
			responseMessage = new HashMap<String, Object>();
			responseMessage.put(ApiConstants.DEVELOPER_MESSAGE, ApiConstants.DEFAULT_NOT_FOUND_MESSAGE);
			responseParamDTO.setMessage(responseMessage);
		}
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getCollectionResourceSessionData(RequestParamDTO requestParamDTO) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, Object> responseMessage = null;
		List<Map<String, Object>> resourceSessionDataList = null; 
		Long sessionId = requestParamDTO.getSessionId();
		if (StringUtils.isNotBlank(requestParamDTO.getCollectionGooruId()) && StringUtils.isNotBlank(requestParamDTO.getClassGooruId())) {
			if (sessionId == null || StringUtils.isBlank(sessionId.toString()) || sessionId.equals(0L)) {
				resourceSessionDataList = classRepository.getResourceAggregatedDataByAllSession(classRepository.getContentId(requestParamDTO.getClassGooruId()),
						classRepository.getContentId(requestParamDTO.getCollectionGooruId()));
			} else {
				resourceSessionDataList = classRepository.getResourceSessionData(classRepository.getContentId(requestParamDTO.getClassGooruId()),
						classRepository.getContentId(requestParamDTO.getCollectionGooruId()), sessionId);
			}
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorConstants.E102, ApiConstants.COLLECTIONGOORUID, ApiConstants.CLASSGOORUID);
		}
		if(resourceSessionDataList != null) {
			responseParamDTO.setContent(resourceSessionDataList);
		} else {
			responseMessage = new HashMap<String, Object>();
			responseMessage.put(ApiConstants.DEVELOPER_MESSAGE, ApiConstants.DEFAULT_NOT_FOUND_MESSAGE);
			responseParamDTO.setMessage(responseMessage);
		}
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getOEResponseData(RequestParamDTO requestParamDTO) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, Object> responseMessage = null;
		List<Map<String, Object>> OESessionDataList = null;
		List<Object[]> userList = null;
		Long sessionId = 0L;
		if (requestParamDTO.getSessionId() != null) {
			sessionId = requestParamDTO.getSessionId();
		}
		if (StringUtils.isNotBlank(requestParamDTO.getCollectionGooruId()) && StringUtils.isNotBlank(requestParamDTO.getClassGooruId()) && StringUtils.isNotBlank(requestParamDTO.getResourceGooruId())) {
			long parentId = classRepository.getContentId(requestParamDTO.getClassGooruId());
			long collectionId = classRepository.getContentId(requestParamDTO.getCollectionGooruId());
			if (sessionId == null || StringUtils.isBlank(sessionId.toString()) || sessionId.equals(0L)) {
				userList = classRepository.fetchSessionActivityUserList(parentId, collectionId);
				OESessionDataList = classRepository.getOEResponseByUser(classRepository.getContentId(requestParamDTO.getClassGooruId()), classRepository.getContentId(requestParamDTO.getCollectionGooruId()), classRepository.getContentId(requestParamDTO.getResourceGooruId()), userList);
			} else {
				OESessionDataList = classRepository.getOEResponseBySession(classRepository.getContentId(requestParamDTO.getClassGooruId()), classRepository.getContentId(requestParamDTO.getCollectionGooruId()), classRepository.getContentId(requestParamDTO.getResourceGooruId()), sessionId);
			}
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorConstants.E102, ApiConstants.COLLECTIONGOORUID, ApiConstants.CLASSGOORUID, ApiConstants.RESOURCEGOORUID);
		}
		if(OESessionDataList != null) {
			responseParamDTO.setContent(OESessionDataList);
		} else {
			responseMessage = new HashMap<String, Object>();
			responseMessage.put(ApiConstants.DEVELOPER_MESSAGE, ApiConstants.DEFAULT_NOT_FOUND_MESSAGE);
			responseParamDTO.setMessage(responseMessage);
		}
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getMasteryReportDataForFirstSession(RequestParamDTO requestParamDTO) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		Map<String, Object> responseMessage = null;
		List<Map<String, Object>> reportDataList = null;
		if (requestParamDTO.getClassGooruId() != null && requestParamDTO.getCollectionGooruId() != null && requestParamDTO.getReportType() != null) {
			long classId = classRepository.getContentId(requestParamDTO.getClassGooruId());
			long collectionId = classRepository.getContentId(requestParamDTO.getCollectionGooruId());
			String reportType = requestParamDTO.getReportType();
			reportDataList = classRepository.getMasteryReportsByFirstSession(collectionId, classId, reportType);
		} else {
			ValidationUtils.rejectInvalidRequest(ErrorConstants.E102, ApiConstants.COLLECTIONGOORUID, ApiConstants.CLASSGOORUID, ApiConstants.REPORTTYPE);
		}
		if(reportDataList != null) {
			responseParamDTO.setContent(reportDataList);
		} else {
			responseMessage = new HashMap<String, Object>();
			responseMessage.put(ApiConstants.DEVELOPER_MESSAGE, ApiConstants.DEFAULT_NOT_FOUND_MESSAGE);
			responseParamDTO.setMessage(responseMessage);
		}
		return responseParamDTO;
	}

	@Override
	public List<SessionActivity> getSessionActivity(Long id){
		return classRepository.getSessionActivity(id);
	}
}
