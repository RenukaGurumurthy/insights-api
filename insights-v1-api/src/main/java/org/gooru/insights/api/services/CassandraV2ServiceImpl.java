package org.gooru.insights.api.services;

import org.gooru.insights.api.daos.CqlCassandraDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;

@Service
public class CassandraV2ServiceImpl implements CassandraV2Service{

	@Autowired
	private CqlCassandraDao cqlDAO;
	
	public CqlResult<String, String> readRows(String columnFamilyName, String query, String... values) {
		return cqlDAO.executeCqlRowsQuery(columnFamilyName, query, values);
	}
	
	public ColumnList<String> readRow(String columnFamilyName, String query, String... values) {
		return cqlDAO.executeCqlRowQuery(columnFamilyName, query, values);
	}
}
