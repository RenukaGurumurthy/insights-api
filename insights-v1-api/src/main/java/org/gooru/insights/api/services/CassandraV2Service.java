package org.gooru.insights.api.services;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface CassandraV2Service {

	ColumnList<String> getUserCurrentLocation(String cfName, String userUid, String classId);

	Rows<String, String> readColumnsWithKey(String cfName, String key);

}
