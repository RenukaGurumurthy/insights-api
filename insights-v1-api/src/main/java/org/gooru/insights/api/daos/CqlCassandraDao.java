package org.gooru.insights.api.daos;

import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface CqlCassandraDao {
	
	CqlResult<String, String> executeCql(String columnFamilyName, String Query);
	
	ColumnList<String> readUserCurrentLocationInClass(String cfName, String userUid, String classId);

	Rows<String, String> readColumnsWithKey(String cfName, String key);
}
