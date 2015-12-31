package org.gooru.insights.api.services;

import com.netflix.astyanax.model.CqlResult;

public interface CassandraV2Service {

	//TODO 	Test code to be removed
	void insertData();
	
	CqlResult<String, String> executeCql(String columnFamilyName, String query);
	
	CqlResult<String, String> readWithCondition(String columnFamilyName, String[][] whereCondition);
	
	CqlResult<String, String> readWithCondition(String columnFamilyName, String whereCondition);

}
