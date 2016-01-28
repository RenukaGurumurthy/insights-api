package org.gooru.insights.api.services;

import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface CassandraV2Service {

	CqlResult<String, String> executeCql(String columnFamilyName, String query);
	
	CqlResult<String, String> readWithCondition(String columnFamilyName, String[][] whereCondition);
	
	CqlResult<String, String> readWithCondition(String columnFamilyName, String whereCondition);

	ColumnList<String> getUserCurrentLocation(String cfName, String userUid, String classId);

	Rows<String, String> readColumnsWithKey(String cfName, String key);
	
	CqlResult<String, String> readWithCondition(String columnFamilyName, String[] fieldNames, String[] values, boolean allowFilter);

	CqlResult<String, String> readWithCondition(String columnFamilyName, String whereCondition, String[] values);
}
