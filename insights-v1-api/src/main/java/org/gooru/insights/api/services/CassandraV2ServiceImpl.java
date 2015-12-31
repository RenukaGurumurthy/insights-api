package org.gooru.insights.api.services;

import org.gooru.insights.api.daos.CqlCassandraDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

@Service
public class CassandraV2ServiceImpl implements CassandraV2Service{

	@Autowired
	private CqlCassandraDao cqlDAO;
	
	@Override
	public ColumnList<String> getUserCurrentLocation(String cfName, String userUid, String classId) {
		return cqlDAO.readUserCurrentLocationInClass(cfName, userUid, classId);
	}
	
	@Override
	public Rows<String, String> readColumnsWithKey(String cfName, String key) {
		return cqlDAO.readColumnsWithKey(cfName, key);
	}
}
