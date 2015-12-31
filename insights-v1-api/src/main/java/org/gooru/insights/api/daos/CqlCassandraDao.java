package org.gooru.insights.api.daos;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface CqlCassandraDao {
	
	ColumnList<String> readUserCurrentLocationInClass(String cfName, String userUid, String classId);

	Rows<String, String> readColumnsWithKey(String cfName, String key);
}
