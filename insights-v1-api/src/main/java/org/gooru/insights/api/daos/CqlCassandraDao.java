package org.gooru.insights.api.daos;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;

public interface CqlCassandraDao {
	
	CqlResult<String, String> executeCqlRowsQuery(String columnFamilyName, String query, String... parameters);
	
	ColumnList<String> executeCqlRowQuery(String columnFamilyName, String query, String... parameters);
	
}
