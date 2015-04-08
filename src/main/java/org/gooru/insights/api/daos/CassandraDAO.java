package org.gooru.insights.api.daos;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface CassandraDAO {

	OperationResult<ColumnList<String>> read(String traceId, String columnFamilyName, String key);
	
	OperationResult<ColumnList<String>> read(String traceId, String columnFamilyName, String key, Collection<String> columnList);

	OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, String column, String value,Collection<String> columnList);
	
	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamilyName, String column,int retryCount);

	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily, String whereColumn, String columnValue, Collection<String> columns,int retryCount);
		
	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamilyName, Map<String,Object> whereColumn,Collection<String> columnSclice,int retryCount);
	
	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamilyName, Collection<String> keys,Collection<String> columns,int retryCount);
		
	OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, String column, String value,int retryCount);

	int getRowCount(String traceId, String columnFamilyName,Map<String,Object> whereCondition,Collection<String> columnList,int retryCount);
	
	int getColumnCount(String traceId, String columnFamilyName, String key);
	
	ColumnFamily<String, String> accessColumnFamily(String columnFamilyName);

	ColumnList<String> getDashBoardKeys(String traceId, String key);
	
	void addRowKeyValues(String traceId, String cfName,String keyName,Map<String,Object> data);
	
	List<Map<String, Object>> getRangeRowCount(String traceId, String columnFamilyName, String startTime, String endTime, String eventName);

	boolean putStringValue(String traceId, String columnFamily,String key,Map<String,String> columns);
	
	void incrementCounterValues(String traceId, String cfName,String keyName,Map<String,Object> data);
	
	void saveProfileSettings(String traceId, String cfName,String keyName,String columnName,String data);

	void saveDefaultProfileSettings(String traceId, String cfName, String keyName,String column, String value);
	
	void deleteRowKey(String traceId, String cfName,String keyName);
	
	void deleteColumnInRow(String traceId, String cfName,String keyName,String columnName);
	
	HashMap<String,String> getMonitorEventProperty(String traceId);
	
	OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, Collection<String> keys);
}
