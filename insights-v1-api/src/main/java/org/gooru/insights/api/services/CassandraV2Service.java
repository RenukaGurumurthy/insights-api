package org.gooru.insights.api.services;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;

public interface CassandraV2Service {

	CqlResult<String, String> readRows(String columnFamilyName, String query, String... values);
	
	ColumnList<String> readRow(String columnFamilyName, String query, String... values);

	ResultSet getSessionInfo(String value);
}
