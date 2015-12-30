package org.gooru.insights.api.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClassV2ServiceImpl implements ClassV2Service{
	
	@Autowired
	private CassandraV2Service cassandraService;

	private CassandraV2Service getCassandraService() {
		return cassandraService;
	}
	
	//TODO 	Test code to be removed
	public void insertClassData() {
		getCassandraService().insertData();
	}
}
