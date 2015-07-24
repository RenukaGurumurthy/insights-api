package org.gooru.insights.api.services;

import java.util.Collection;
import java.util.Map;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface CassandraService {

	OperationResult<ColumnList<String>> read(String traceId, String columnFamilyName, String key);

	OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, Collection<String> keys);

	OperationResult<ColumnList<String>> read(String traceId, String columnFamilyName, String key, Collection<String> columnList);

	OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, String columnName, String columnValue, Collection<String> columnList);

	OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, String columnName, String columnValue);
	
	OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, String columnName, int columnValue);

	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamilyName, String columnName);

	OperationResult<ColumnList<String>> getClassPageUsage(String traceId, String columnFamily, String prefix, String rowKey, String suffix, Collection<String> columns);

	OperationResult<Rows<String, String>> getClassPageResouceUsage(String traceId, String columnFamily, String columnName, String value);

	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily, String prefix, String rowKey, String suffix, Collection<String> columnNames);

	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily, Map<String, Object> whereColumn, Collection<String> columns);

	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily, String whereColumn, String columnValue, Collection<String> columns);

	int getRowCount(String traceId, String columnFamilyName, Map<String, Object> whereCondition, Collection<String> columnList);

	int getColumnCount(String traceId, String columnFamilyName, String key);

	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily, String prefix, String rowKey, Collection<String> suffix, Collection<String> columnNames);

	Map<String, String> getMonitorEventProperty(String traceId);

	OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily, Collection<String> rowKey, Collection<String> columnNames);

	void addRowKeyValues(String traceId, String cfName, String keyName, Map<String, Object> data);

	void addCounterRowKeyValues(String traceId, String cfName, String keyName, Map<String, Object> data);

	void saveProfileSettings(String traceId, String cfName, String userId, String profileId, String data);

	void deleteColumnInRow(String traceId, String cfName, String key, String column);

	void updateDefaultProfileSettings(String traceId, String cfName, String keyName, String column, String value);

	void deleteRowKey(String traceId, String cfName, String keyName);

	boolean putStringValue(String traceId, String columnFamily, String key, Map<String, String> columns);
	
	ColumnList<String> getDashBoardKeys(String traceId, String key);
	
	ColumnList<String> getConfigKeys(String key);
}
