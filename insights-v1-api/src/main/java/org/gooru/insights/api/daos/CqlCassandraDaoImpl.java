package org.gooru.insights.api.daos;



import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.utils.InsightsLogger;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
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

	public CqlResult<String, String> executeCqlRowsQuery(String columnFamilyName, String query, String... parameters) {
	
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
	
	public ColumnList<String> executeCqlRowQuery(String columnFamilyName, String query, String... parameters) {
		
		OperationResult<CqlResult<String, String>> result = null;
		try {
			PreparedCqlQuery<String, String> cqlQuery = getColumnFamilyQuery(columnFamilyName).withCql(query).asPreparedStatement();
			cqlQuery = setParameters(cqlQuery, parameters);
			result = cqlQuery.execute();
		} catch (ConnectionException e) {
			InsightsLogger.error("CQL Exception:"+query, e);
		}
		Rows<String, String> resultRows = result != null ? result.getResult().getRows() : null;
		return (resultRows != null && resultRows.size() > 0) ? resultRows.getRowByIndex(0).getColumns() : null ;
	}
	
	private ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {
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
	
	public ResultSet executeCqlRowsQuery(String parameters) {
		Statement select = QueryBuilder.select().all()
				.from("event_logger_insights", "user_session_activity")
				.where(QueryBuilder.eq("session_id", parameters));
		return getCassSession().execute(select);
	}
}
