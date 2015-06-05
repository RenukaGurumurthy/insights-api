package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.RequestParamDTO;
import org.gooru.insights.api.repository.ClassRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClassServiceImpl implements ClassService,InsightsConstant {

	@Autowired
	private ClassRepository classRepository;
	
	
	@Override
	public String getTitle(Integer contentId) {
		return null;
	}

	@Override
	public List<Map<String, Object>> getSessions(RequestParamDTO requestParamDTO) {		
		
		List<Map<String, Object>> sessionData = new ArrayList<Map<String, Object>>();
		
		List<Object[]> sessions = classRepository.getSession(classRepository.getContentId(requestParamDTO.getClassGooruId()), classRepository.getContentId(requestParamDTO.getCollectionGooruId()), requestParamDTO.getUserUid());
		for(Object[] session : sessions){
			Map<String,Object> sessionMap = new HashMap<String, Object>();
			sessionMap.put(SESSION_ID, session[0]);
			sessionMap.put(START_TIME, session[1]);
			sessionMap.put(SEQUENCE, session[2]);
			sessionData.add(sessionMap);
		}	
		return sessionData;
		
	}
}
