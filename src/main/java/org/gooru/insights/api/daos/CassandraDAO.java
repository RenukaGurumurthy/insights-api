/*******************************************************************************
 * CassandraDAO.java
 * insights-read-api
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.gooru.insights.api.daos;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface CassandraDAO {

	Map<String, Object> getRowKey(String columnFamilyName, Map<String, String> filterSet, Integer limit, String offset);

	List<Map<String, Object>> getColumnListUsingKey(String columnFamilyName, String key, Map<String, String> filterSet, Integer limit, String prefix, String suffix, Collection<String> columnList,
			Map<String, String> whereCondition, Collection<String> rawGooruOId);

	public List<Map<String, Object>> getRowsUsingKey(String columnFamilyName, Collection<String> keys, Map<String, String> filterSet, Integer limit, String prefix, String suffix,
			Collection<String> columnList, Map<String, String> whereCondition, Collection<String> rawGooruOId);

	OperationResult<ColumnList<String>> read(String keyspace, String columnFamilyName, String key);
	
	OperationResult<ColumnList<String>> read(String keyspace, String columnFamilyName, String key, Collection<String> columnList);

	OperationResult<Rows<String, String>> read(String keyspace, String columnFamilyName, String value, String column);
	
	OperationResult<Rows<String, String>> readAll(String keyspace, String columnFamilyName, String column);

	void markDeleteStatus();
	
	void markDeleteStatus(String startTime,String endTime);	
}
