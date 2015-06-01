package org.gooru.insights.api.services;

import java.util.Map;


public interface BaseService {
	boolean notNull(String parameter);
	
	boolean notNull(Integer parameter);
	
	boolean notNull(Map<?,?> request);

}
