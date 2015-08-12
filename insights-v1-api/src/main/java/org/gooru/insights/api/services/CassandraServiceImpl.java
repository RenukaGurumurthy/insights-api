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

	public OperationResult<ColumnList<String>> read(String columnFamilyName, String key) {
		return cassandraDAO.read(columnFamilyName, key);
	}

	public OperationResult<Rows<String, String>> read(String columnFamilyName, Collection<String> key) {
		return cassandraDAO.read(columnFamilyName, key);
	}
	
	public OperationResult<ColumnList<String>> read(String columnFamilyName, String key, Collection<String> columnList) {
		return cassandraDAO.read(columnFamilyName, key, columnList);
	}

	public OperationResult<Rows<String, String>> readAll(String columnFamilyName, String columnName) {
		return cassandraDAO.readAll(columnFamilyName, columnName,0);
	}
	
	public OperationResult<Rows<String, String>> read(String columnFamilyName, String columnName, String columnValue,Collection<String> columnList) {
		return cassandraDAO.read(columnFamilyName, columnName, columnValue,columnList);
	}
	
	public OperationResult<Rows<String, String>> read(String columnFamilyName, String columnName, String columnValue) {
		return cassandraDAO.read(columnFamilyName, columnName, columnValue,0);
	}

	public OperationResult<Rows<String, String>> read(String columnFamilyName, String columnName, int columnValue) {
		return cassandraDAO.read(columnFamilyName, columnName, columnValue,0);
	}
	
	public int getRowCount(String columnFamilyName, Map<String,Object> whereCondition,Collection<String> columnList){
		return cassandraDAO.getRowCount(columnFamilyName,whereCondition, columnList,0);
	}
	
	public int getColumnCount(String columnFamilyName, String key){
		return cassandraDAO.getColumnCount(columnFamilyName, key);
	}
	
	public OperationResult<ColumnList<String>> getClassPageUsage(String columnFamily,String prefix,String rowKey,String suffix,Collection<String> columnNames){
	
		return cassandraDAO.read(columnFamily, buildKey(prefix, rowKey, suffix),columnNames);
	}

	public OperationResult<Rows<String, String>> readAll(String columnFamily,String prefix,String rowKey,String suffix,Collection<String> columnNames){
		
		return cassandraDAO.readAll(columnFamily, getBaseService().generateCommaSeparatedStringToKeys( rowKey,prefix, suffix),columnNames,0);
	}
	
	public OperationResult<Rows<String, String>> readAll(String columnFamily,String prefix,String rowKey,Collection<String> suffix,Collection<String> columnNames){
		
		return cassandraDAO.readAll(columnFamily, getBaseService().generateCommaSeparatedStringToKeys( rowKey,prefix, suffix),columnNames,0);
	}
	
	public OperationResult<Rows<String, String>> getClassPageResouceUsage(String columnFamily,String columnName,String value){
		
		return cassandraDAO.read(columnFamily, columnName,value,0);
	}
	
	public OperationResult<Rows<String, String>> readAll(String columnFamily,Map<String,Object> whereColumn,Collection<String> columns){
		
		return cassandraDAO.readAll(columnFamily, whereColumn,columns,0);

	}
	
	public OperationResult<Rows<String, String>> readAll(String columnFamily,Collection<String> rowKey,Collection<String> columnNames){
		
		return cassandraDAO.readAll(columnFamily, rowKey,columnNames,0);

	}
	
	public boolean putStringValue(String columnFamily,String key,Map<String,String> columns){
		
		return cassandraDAO.putStringValue(columnFamily,key, columns);

	}

	public ColumnList<String> getConfigKeys(String key){
		return cassandraDAO.getConfigKeys(key);
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
	public OperationResult<Rows<String, String>> readAll(String columnFamily, String whereColumn, String columnValue, Collection<String> columns) {
		return cassandraDAO.readAll(columnFamily, whereColumn,columnValue,columns,0);

	}

	public void addRowKeyValues(String cfName,String keyName,Map<String,Object> data){
		cassandraDAO.addRowKeyValues(cfName,keyName,data);
	}
	
	public void addCounterRowKeyValues(String cfName,String keyName,Map<String,Object> data) {
		cassandraDAO.incrementCounterValues(cfName,keyName,data);
	}
	
	public void saveProfileSettings(String cfName,String keyName,String columnName,String data) {
		cassandraDAO.saveProfileSettings(cfName,keyName,columnName,data);
	}
	
	public void updateDefaultProfileSettings(String cfName, String keyName,String column, String value) {
		cassandraDAO.saveDefaultProfileSettings(cfName,keyName,column,value);
	}
	
	public void deleteRowKey(String cfName,String keyName) {
		cassandraDAO.deleteRowKey(cfName,keyName);
	}
	
	public void deleteColumnInRow(String cfName,String keyName,String columnName) {
		cassandraDAO.deleteColumnInRow(cfName, keyName,columnName);
	}
	
	public Map<String,String> getMonitorEventProperty() {
		return cassandraDAO.getMonitorEventProperty();
	}

	public ColumnList<String> getDashBoardKeys(String key) {
		return cassandraDAO.getDashBoardKeys(key);
	}
	 
	private BaseService getBaseService() {
		return baseService;
	}
	
}
