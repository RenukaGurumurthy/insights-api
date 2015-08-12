package org.gooru.insights.api.services;

import java.util.Collection;
import java.util.Map;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface CassandraService {

	OperationResult<ColumnList<String>> read(String columnFamilyName, String key);

	OperationResult<Rows<String, String>> read(String columnFamilyName, Collection<String> keys);

	OperationResult<ColumnList<String>> read(String columnFamilyName, String key, Collection<String> columnList);

	OperationResult<Rows<String, String>> read(String columnFamilyName, String columnName, String columnValue, Collection<String> columnList);

	OperationResult<Rows<String, String>> read(String columnFamilyName, String columnName, String columnValue);
	
	OperationResult<Rows<String, String>> read(String columnFamilyName, String columnName, int columnValue);

	OperationResult<Rows<String, String>> readAll(String columnFamilyName, String columnName);

	OperationResult<ColumnList<String>> getClassPageUsage(String columnFamily, String prefix, String rowKey, String suffix, Collection<String> columns);

	OperationResult<Rows<String, String>> getClassPageResouceUsage(String columnFamily, String columnName, String value);

	OperationResult<Rows<String, String>> readAll(String columnFamily, String prefix, String rowKey, String suffix, Collection<String> columnNames);

	OperationResult<Rows<String, String>> readAll(String columnFamily, Map<String, Object> whereColumn, Collection<String> columns);

	OperationResult<Rows<String, String>> readAll(String columnFamily, String whereColumn, String columnValue, Collection<String> columns);

	int getRowCount(String columnFamilyName, Map<String, Object> whereCondition, Collection<String> columnList);

	int getColumnCount(String columnFamilyName, String key);

	OperationResult<Rows<String, String>> readAll(String columnFamily, String prefix, String rowKey, Collection<String> suffix, Collection<String> columnNames);

	Map<String, String> getMonitorEventProperty();

	OperationResult<Rows<String, String>> readAll(String columnFamily, Collection<String> rowKey, Collection<String> columnNames);

	void addRowKeyValues(String cfName, String keyName, Map<String, Object> data);

	void addCounterRowKeyValues(String cfName, String keyName, Map<String, Object> data);

	void saveProfileSettings(String cfName, String userId, String profileId, String data);

	void deleteColumnInRow(String cfName, String key, String column);

	void updateDefaultProfileSettings(String cfName, String keyName, String column, String value);

	void deleteRowKey(String cfName, String keyName);

	boolean putStringValue(String columnFamily, String key, Map<String, String> columns);
	
	ColumnList<String> getDashBoardKeys(String key);
	
	ColumnList<String> getConfigKeys(String key);
}
