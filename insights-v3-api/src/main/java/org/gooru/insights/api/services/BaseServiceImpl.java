package org.gooru.insights.api.services;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BaseServiceImpl implements BaseService {

	@Autowired
	private CassandraService cassandraService;
	
	public boolean notNull(String parameter) {

		if (StringUtils.trimToNull(parameter) != null) {
			return true;
		} 
		return false;
	}
	
	public boolean notNull(Map<?, ?> request) {

		if (request != null && (!request.isEmpty())) {
			return true;
		}
		return false;
	}

	public boolean notNull(Integer parameter) {

		if (parameter != null && parameter.SIZE > 0 && (!parameter.toString().isEmpty())) {
			return true;
		}
		return false;
	}
}
