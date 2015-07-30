package org.gooru.insights.api.services;

import java.util.Collection;
import java.util.Map;

import org.gooru.insights.api.daos.CassandraDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

@Service
public class CassandraServiceImpl implements CassandraService{

	@Autowired
	private BaseService baseService;

	@Autowired
	private CassandraDAO cassandraDAO;

	public OperationResult<ColumnList<String>> read(String traceId, String columnFamilyName, String key) {
		return cassandraDAO.read(traceId, columnFamilyName, key);
	}

	public OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, Collection<String> key) {
		return cassandraDAO.read(traceId, columnFamilyName, key);
	}
	
	public OperationResult<ColumnList<String>> read(String traceId, String columnFamilyName, String key, Collection<String> columnList) {
		return cassandraDAO.read(traceId,columnFamilyName, key, columnList);
	}

	public OperationResult<Rows<String, String>> readAll(String traceId, String columnFamilyName, String columnName) {
		return cassandraDAO.readAll(traceId, columnFamilyName, columnName,0);
	}
	
	public OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, String columnName, String columnValue,Collection<String> columnList) {
		return cassandraDAO.read(traceId, columnFamilyName, columnName, columnValue,columnList);
	}
	
	public OperationResult<Rows<String, String>> read(String traceId, String columnFamilyName, String columnName, String columnValue) {
		return cassandraDAO.read(traceId, columnFamilyName, columnName, columnValue,0);
	}

	public int getRowCount(String traceId, String columnFamilyName, Map<String,Object> whereCondition,Collection<String> columnList){
		return cassandraDAO.getRowCount(traceId, columnFamilyName,whereCondition, columnList,0);
	}
	
	public int getColumnCount(String traceId, String columnFamilyName, String key){
		return cassandraDAO.getColumnCount(traceId, columnFamilyName, key);
	}
	
	public OperationResult<ColumnList<String>> getClassPageUsage(String traceId, String columnFamily,String prefix,String rowKey,String suffix,Collection<String> columnNames){
	
		return cassandraDAO.read(traceId, columnFamily, buildKey(prefix, rowKey, suffix),columnNames);
	}

	public OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily,String prefix,String rowKey,String suffix,Collection<String> columnNames){
		
		return cassandraDAO.readAll(traceId, columnFamily, getBaseService().generateCommaSeparatedStringToKeys( rowKey,prefix, suffix),columnNames,0);
	}
	
	public OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily,String prefix,String rowKey,Collection<String> suffix,Collection<String> columnNames){
		
		return cassandraDAO.readAll(traceId, columnFamily, getBaseService().generateCommaSeparatedStringToKeys( rowKey,prefix, suffix),columnNames,0);
	}
	
	public OperationResult<Rows<String, String>> getClassPageResouceUsage(String traceId, String columnFamily,String columnName,String value){
		
		return cassandraDAO.read(traceId, columnFamily, columnName,value,0);
	}
	
	public OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily,Map<String,Object> whereColumn,Collection<String> columns){
		
		return cassandraDAO.readAll(traceId, columnFamily, whereColumn,columns,0);

	}
	
	public OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily,Collection<String> rowKey,Collection<String> columnNames){
		
		return cassandraDAO.readAll(traceId, columnFamily, rowKey,columnNames,0);

	}
	
	public boolean putStringValue(String traceId, String columnFamily,String key,Map<String,String> columns){
		
		return cassandraDAO.putStringValue(traceId, columnFamily,key, columns);

	}

	/**
	 * 
	 * @param prefix
	 * @param rowKey
	 * @param suffix
	 * @return Build key with proper prefix,rowKey and suffix values
	 */
	public String buildKey(String prefix, String rowKey, String suffix) {
		
		String key = "";
		if (getBaseService().notNull(prefix)) {
			key += prefix;
		}
		key += rowKey;
		if (getBaseService().notNull(suffix)) {
			key += suffix;
		}
		
		return key;
	}

	@Override
	public OperationResult<Rows<String, String>> readAll(String traceId, String columnFamily, String whereColumn, String columnValue, Collection<String> columns) {
		return cassandraDAO.readAll(traceId, columnFamily, whereColumn,columnValue,columns,0);

	}

	public void addRowKeyValues(String traceId, String cfName,String keyName,Map<String,Object> data){
		cassandraDAO.addRowKeyValues(traceId, cfName,keyName,data);
	}
	
	public void addCounterRowKeyValues(String traceId, String cfName,String keyName,Map<String,Object> data) {
		cassandraDAO.incrementCounterValues(traceId, cfName,keyName,data);
	}
	
	public void saveProfileSettings(String traceId, String cfName,String keyName,String columnName,String data) {
		cassandraDAO.saveProfileSettings(traceId, cfName,keyName,columnName,data);
	}
	
	public void updateDefaultProfileSettings(String traceId, String cfName, String keyName,String column, String value) {
		cassandraDAO.saveDefaultProfileSettings(traceId, cfName,keyName,column,value);
	}
	
	public void deleteRowKey(String traceId, String cfName,String keyName) {
		cassandraDAO.deleteRowKey(traceId, cfName,keyName);
	}
	
	public void deleteColumnInRow(String traceId, String cfName,String keyName,String columnName) {
		cassandraDAO.deleteColumnInRow(traceId, cfName, keyName,columnName);
	}
	
	public Map<String,String> getMonitorEventProperty(String traceId) {
		return cassandraDAO.getMonitorEventProperty(traceId);
	}

	public ColumnList<String> getDashBoardKeys(String traceId, String key) {
		return cassandraDAO.getDashBoardKeys(traceId, key);
	}
	 
	private BaseService getBaseService() {
		return baseService;
	}
	
}
