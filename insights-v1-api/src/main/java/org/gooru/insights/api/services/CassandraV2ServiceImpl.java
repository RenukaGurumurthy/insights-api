package org.gooru.insights.api.services;

import org.apache.commons.lang3.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.daos.CqlCassandraDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

@Service
public class CassandraV2ServiceImpl implements CassandraV2Service{

	private static final String SELECT = " SELECT ";
	
	private static final String FROM = " FROM ";
	
	private static final String WHERE = " WHERE ";
	
	private static final String AND = " AND ";
	
	private static final String ASTRIX = "*";
	
	private static final String KEY = "KEY";
	
	private static final String EQUAL = "=";
	
	private static final String QUOTES = "'";
	
	private static final String ALLOW_FILTERING = " ALLOW FILTERING";

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
	
	public CqlResult<String, String> executeCql(String columnFamilyName, String Query) {
		return cqlDAO.executeCql(columnFamilyName, Query);
	}
	
	public CqlResult<String, String> readWithCondition(String columnFamilyName, String[][] whereCondition) {
		return cqlDAO.executeCql(columnFamilyName, queryBuilder(SELECT,ASTRIX,FROM,columnFamilyName, appendWhere(whereCondition)));
	}
	
	public CqlResult<String, String> readWithCondition(String columnFamilyName, String whereCondition) {
		return cqlDAO.executeCql(columnFamilyName, queryBuilder(SELECT,ASTRIX,FROM,columnFamilyName, whereCondition));
	}
	
	public CqlResult<String, String> readWithCondition(String columnFamilyName, String[] fieldNames, String[] values, boolean allowFilter) {
		return cqlDAO.executeCqlQuery(columnFamilyName, queryBuilder(SELECT,ASTRIX,FROM,columnFamilyName, appendWhere(fieldNames, values, allowFilter)), values);
	}
	
	public CqlResult<String, String> readWithCondition(String columnFamilyName, String whereCondition, String[] values) {
		return cqlDAO.executeCqlQuery(columnFamilyName, queryBuilder(SELECT,ASTRIX,FROM,columnFamilyName, whereCondition), values);
	}
	
	private String queryBuilder(String... fields) {
		StringBuffer queryBuilder = new StringBuffer();
		for(int fieldCount =0; fieldCount < fields.length; fieldCount++) {
			queryBuilder.append(fields[fieldCount]);
		}
		return queryBuilder.toString();
	}
	
	public static String appendWhere(Object[][] data) {
		StringBuffer stringBuffer = new StringBuffer();
		for(int keyIndex = 0; keyIndex < data.length; keyIndex++) {
			if(data[keyIndex][1] !=null && StringUtils.isNotBlank(data[keyIndex][1].toString())) {
				stringBuffer.append(stringBuffer.length() > 0 ? AND : WHERE);
				stringBuffer.append(data[keyIndex][0]);
				stringBuffer.append(EQUAL);
				stringBuffer.append(QUOTES);
				stringBuffer.append(data[keyIndex][1]);
				stringBuffer.append(QUOTES);
			}
		}
		stringBuffer.append(ALLOW_FILTERING);
		return stringBuffer.toString();
	}
	
	public static String appendWhere(String[] field, boolean allowFilter) {
		StringBuffer stringBuffer = new StringBuffer();
		for(int fieldIndex = 0; fieldIndex < field.length; fieldIndex++) {
			if(StringUtils.isNotBlank(field[fieldIndex])) {
				stringBuffer.append(stringBuffer.length() > 0 ? AND : WHERE);
				stringBuffer.append(field[fieldIndex]);
				stringBuffer.append(EQUAL);
				stringBuffer.append(ApiConstants.QUESTION_MARK);
			}
		}
		if(allowFilter == true) {
			stringBuffer.append(ALLOW_FILTERING);
		}
		return stringBuffer.toString();
	}
	
	public static String appendWhere(String[] field, String[] value, boolean allowFilter) {
		StringBuffer stringBuffer = new StringBuffer();
		for(int fieldIndex = 0; fieldIndex < field.length; fieldIndex++) {
			if(StringUtils.isNotBlank(field[fieldIndex]) && StringUtils.isNotBlank(value[fieldIndex])) {
				stringBuffer.append(stringBuffer.length() > 0 ? AND : WHERE);
				stringBuffer.append(field[fieldIndex]);
				stringBuffer.append(EQUAL);
				stringBuffer.append(ApiConstants.QUESTION_MARK);
			}
		}
		if(allowFilter == true) {
			stringBuffer.append(ALLOW_FILTERING);
		}
		return stringBuffer.toString();
	}
	
	@Override
	public CqlResult<String, String> getAllUserLocationInClass(String cfName, String classUid) {
		return cqlDAO.readPeers(cfName, classUid);
	}
}
