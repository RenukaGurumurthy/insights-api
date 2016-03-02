package org.gooru.insights.api.daos;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;

public interface CqlCassandraDao {
	
	CqlResult<String, String> executeCqlRowsQuery(String columnFamilyName, String query, String... parameters);
	
	ColumnList<String> executeCqlRowQuery(String columnFamilyName, String query, String... parameters);

	ResultSet executeCqlRowsQuery(String parameters);
	
}
