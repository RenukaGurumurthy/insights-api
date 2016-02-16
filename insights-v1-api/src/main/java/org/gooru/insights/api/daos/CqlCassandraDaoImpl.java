package org.gooru.insights.api.daos;


import org.apache.commons.lang3.StringUtils;
import org.gooru.insights.api.utils.InsightsLogger;
import org.springframework.stereotype.Repository;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.PreparedCqlQuery;
import com.netflix.astyanax.serializers.StringSerializer;

@Repository
public class CqlCassandraDaoImpl extends CassandraConnectionProvider implements CqlCassandraDao {

	private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;

	final static String GET_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ? AND user_uid = ? ";
	
	final static String GET_ALL_USER_CURRENT_LOCATION_IN_CLASS = "SELECT * FROM student_location WHERE class_uid = ? ";
	
	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {
		return new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());
	}
	
	private ColumnFamilyQuery<String, String> getColumnFamilyQuery(String columnFamilyName) {
		return getLogKeyspace().prepareQuery(accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	}
	
	private PreparedCqlQuery<String, String> setParameters(PreparedCqlQuery<String, String> cqlQuery, String... parameters) {
		
		for(int parameterCount = 0; parameterCount < parameters.length; parameterCount++) {
			if(StringUtils.isNotEmpty(parameters[parameterCount])) {
				cqlQuery.withStringValue(parameters[parameterCount]);	
			}
		}
		return cqlQuery;
	}
	
	public CqlResult<String, String> executeCqlQuery(String columnFamilyName, String query, String... parameters) {
	
		OperationResult<CqlResult<String, String>> result = null;
		try {
			PreparedCqlQuery<String, String> cqlQuery = getColumnFamilyQuery(columnFamilyName).withCql(query).asPreparedStatement();
			cqlQuery = setParameters(cqlQuery, parameters);
			result = cqlQuery.execute();
		} catch (ConnectionException e) {
			InsightsLogger.error("CQL Exception:"+query, e);
		}
		return result != null ? result.getResult() : null;	
	}
	
	@Override
	public ColumnList<String> readUserCurrentLocationInClass(String cfName, String userUid, String classId){
		OperationResult<CqlResult<String, String>> result = null;
		try {
			result = getLogKeyspace()
		       .prepareQuery(accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		       .withCql(GET_USER_CURRENT_LOCATION_IN_CLASS)
		       .asPreparedStatement()
			       .withStringValue(classId)
			       .withStringValue(userUid)
		       .execute();
		} catch (ConnectionException e) {
			InsightsLogger.error("CQL Exception:", e);
		}
		Rows<String, String> resultRows = result.getResult().getRows();
		return resultRows.size() > 0 ? resultRows.getRowByIndex(0).getColumns() : null ;
	}
	
	@Override
	public Rows<String, String> readColumnsWithKey(String cfName, String key){
		OperationResult<CqlResult<String, String>> result = null;
		try {
			result = getLogKeyspace()
		       .prepareQuery(accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		       .withCql("SELECT * FROM " + cfName + " WHERE row_key = ?;")
		       .asPreparedStatement()
			       .withStringValue(key)
		       .execute();
		} catch (ConnectionException e) {
			InsightsLogger.error("CQL Exception:", e);
		}
		return result.getResult().getRows() ;
	}
	
	public CqlResult<String, String> executeCql(String columnFamilyName, String query) {
		OperationResult<CqlResult<String, String>> result = null;
		try {
			result = getLogKeyspace()
			        .prepareQuery(accessColumnFamily(columnFamilyName))
			        .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			        .withCql(query).asPreparedStatement()
			        .execute();
		} catch (ConnectionException e) {
			InsightsLogger.error("CQL Exception:"+query, e);
		}
		return result != null ? result.getResult() : null;
	}
	
	public CqlResult<String, String> readPeers(String cfName, String classId) {
		OperationResult<CqlResult<String, String>> result = null;
		try {
			result = getLogKeyspace().prepareQuery(accessColumnFamily(cfName))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withCql(GET_ALL_USER_CURRENT_LOCATION_IN_CLASS).asPreparedStatement()
					.withStringValue(classId)
					.execute();
		} catch (ConnectionException e) {
			InsightsLogger.error("CQL Exception:" + GET_ALL_USER_CURRENT_LOCATION_IN_CLASS, e);
		}
		return result != null ? result.getResult() : null;
	}
	
}
