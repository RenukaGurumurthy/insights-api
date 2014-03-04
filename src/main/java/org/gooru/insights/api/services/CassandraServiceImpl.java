/*******************************************************************************
 * CassandraServiceImpl.java
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
package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.insights.api.daos.CassandraDAO;
import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.models.RequestParamsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

@Service
public class CassandraServiceImpl implements CassandraService, InsightsConstant {

	@Autowired
	private BaseService baseService;

	@Autowired
	private SelectParamsService selectParamsService;

	@Autowired
	private CassandraDAO cassandraDAO;

	String gooruSearchKeyspaceCli = "goorusearch.keyspace";
	String logKeyspaceCli = "log.keyspace";

	public List<Map<String, Object>> getCollectionData(RequestParamsDTO requestParamsDTO, Map<String, String> hibernateSelectValues, String collectionId, List<Map<String, String>> errorData,
			Map<String, String> selectValues) {

		Map<String, String> errorMessage = new HashMap<String, String>();
		Map<String, Integer> checkTables = new HashMap<String, Integer>();
		boolean fetchResourceDetail = false;
		String selectFields = hibernateSelectValues.get("select");
		String InvalidFields = hibernateSelectValues.get("InValidParameters");
		String requestFields = hibernateSelectValues.get("requestedValues");
		AuthenticateTable(requestFields, checkTables);
		int innerJoin = 0;
		List<Map<String, Object>> aggregates = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> collectionInfo = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> rawData = new ArrayList<Map<String, Object>>();
		Map<String, String> whereCondition = new HashMap<String, String>();
		Collection<String> rawGooruOId = new ArrayList<String>();

		if (getBaseService().checkNull(InvalidFields)) {
			errorMessage.put("checkDocument", InvalidFields);
		}

		if (requestFields.contains("resource")) {
			fetchResourceDetail = true;
		}

		if (getBaseService().checkNull(collectionId)) {

			if (fetchResourceDetail) {
				if (checkTables.get("rawTable") > 0) {
					innerJoin++;
					whereCondition.put("collection_gooru_oid", collectionId);
					String collectionSelect = selectFields + ",collection_gooru_oid,resource_gooru_oid";
					selectValues.put("parent_gooru_oid", "collection_gooru_oid");
					selectValues.put("gooru_oid", "resource_gooru_oid");
					collectionInfo = appendProperKey(
							getCassandraDAO().getColumnListUsingKey("collection_item", null, null, null, null, null, this.convertStringToCollection(collectionSelect), whereCondition, rawGooruOId),
							selectFields, requestFields, fetchResourceDetail, selectValues);
					selectValues.put("itemSequence", "itemSequence");
					selectValues.put("status", "status");
					selectValues.remove("parent_gooru_oid");
					selectValues.remove("gooru_oid");
					whereCondition = new HashMap<String, String>();
					if (collectionInfo != null) {
						rawData = getCassandraDAO().getRowsUsingKey("dim_resource", rawGooruOId, null, null, "GLP~", "", this.convertStringToCollection(selectFields), whereCondition, rawGooruOId);

						rawData = appendProperKey(this.LeftJoin(collectionInfo, rawData, "gooru_oid", "gooru_oid"), selectFields, requestFields, fetchResourceDetail, selectValues);
					} else {
						innerJoin = 0;
						checkTables.put("aggregate", 0);
					}
				}
				whereCondition.put("parent_gooru_oid", collectionId);
				Collection<String> rowKey = new ArrayList<String>();
				for (String value : rawGooruOId) {
					rowKey.add("R~" + collectionId + "~" + value);
				}
				if (checkTables.get("aggregate") > 0) {

					innerJoin++;
					selectValues.remove("parent_gooru_oid");
					aggregates = appendProperKey(
							getCassandraDAO().getRowsUsingKey("agg_event_collection_resource", rowKey, null, null, "", "", this.convertStringToCollection(selectFields), null, rawGooruOId),
							selectFields, requestFields, fetchResourceDetail, selectValues);

					// aggregates =
					// appendProperKey(getCassandraDAO().getColumnListUsingKey("agg_event_collection_resource",
					// null, null, null, null, null,
					// this.convertStringToCollection(selectFields),null,rawGooruOId),
					// selectFields, requestFields, fetchResourceDetail,
					// selectValues);
					if (aggregates == null) {
						innerJoin--;
					}
				}

				if (innerJoin > 1) {
					return LeftJoin(rawData, aggregates, "resourceGooruOId", "resourceGooruOId");
				} else {
					if (getBaseService().checkNull(aggregates.toString())) {
						return aggregates;
					} else {
						return rawData;
					}

				}
			} else {

				if (checkTables.get("rawTable") > 0) {
					innerJoin++;
					rawGooruOId.add(collectionId);
					rawData = appendProperKey(
							getCassandraDAO().getRowsUsingKey("dim_resource", rawGooruOId, null, null, "GLP~", "", this.convertStringToCollection(selectFields), whereCondition, rawGooruOId),
							selectFields, requestFields, fetchResourceDetail, selectValues);
				}
				if (checkTables.get("aggregate") > 0) {
					innerJoin++;
					if (selectFields.contains("averageGrade")) {
						selectFields += ",total_qn_count,total_correct_ans";
					}
					aggregates = appendProperKey(
							getCassandraDAO().getColumnListUsingKey("agg_event_collection_resource", collectionId, null, null, "C~", null, this.convertStringToCollection(selectFields), null,
									rawGooruOId), selectFields, requestFields, fetchResourceDetail, selectValues);
				}
				if (innerJoin > 1) {
					return rightJoin(aggregates, rawData, "gooruOId", "gooruOId");
				} else {
					if (getBaseService().checkNull(aggregates.toString())) {
						return aggregates;
					} else {
						return rawData;
					}

				}
			}

		} else {
			if (checkTables.get("aggregate") > 0) {
				innerJoin++;
				return appendProperKey(
						getCassandraDAO().getColumnListUsingKey("agg_event_collection_resource", null, null, null, null, null, this.convertStringToCollection(selectFields), null, rawGooruOId),
						selectFields, requestFields, fetchResourceDetail, selectValues);

			}
			if (checkTables.get("rawTable") > 0) {
				innerJoin++;
				rawData = appendProperKey(
						getCassandraDAO().getRowsUsingKey("dim_resource", rawGooruOId, null, null, "GLP~", "", this.convertStringToCollection(selectFields), whereCondition, rawGooruOId),
						selectFields, requestFields, fetchResourceDetail, selectValues);
			}
			if (innerJoin > 1) {
				return rightJoin(aggregates, rawData, "gooruOId", "gooruOId");
			} else {
				if (getBaseService().checkNull(aggregates.toString())) {
					return aggregates;
				} else {
					return rawData;
				}

			}
		}
	}

	public BaseService getBaseService() {
		return baseService;
	}

	public SelectParamsService getSelectParamsService() {
		return selectParamsService;
	}

	public CassandraDAO getCassandraDAO() {
		return cassandraDAO;
	}

	public Collection<String> convertStringToCollection(String data) {
		Collection<String> collection = new ArrayList<String>();
		for (String value : data.split(",")) {
			collection.add(value);
		}
		return collection;
	}

	public List<Map<String, Object>> InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String commonKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (!child.isEmpty() && !parent.isEmpty()) {
			for (Map<String, Object> childEntry : child) {
				Map<String, Object> appended = new HashMap<String, Object>();
				for (Map<String, Object> parentEntry : parent) {
					if (childEntry.containsKey(commonKey) && parentEntry.containsKey(commonKey)) {
						if (childEntry.get(commonKey).equals(parentEntry.get(commonKey))) {
							childEntry.remove(commonKey);
							appended.putAll(childEntry);
							appended.putAll(parentEntry);
							break;
						}
					}
				}
				resultList.add(appended);
			}
			return resultList;
		}
		return resultList;
	}

	public List<Map<String, Object>> InnerJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> childEntry : child) {
			Map<String, Object> appended = new HashMap<String, Object>();
			for (Map<String, Object> parentEntry : parent) {
				if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey)) {
					if (childEntry.get(childKey).equals(parentEntry.get(parentKey))) {
						appended.putAll(childEntry);
						appended.putAll(parentEntry);
						break;
					}
				}
			}
			resultList.add(appended);
		}
		return resultList;
	}

	public List<Map<String, Object>> rightJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> childEntry : child) {
			boolean occured = false;
			Map<String, Object> appended = new HashMap<String, Object>();
			for (Map<String, Object> parentEntry : parent) {
				if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey)) {
					if (childEntry.get(childKey).equals(parentEntry.get(parentKey))) {
						occured = true;
						appended.putAll(childEntry);
						appended.putAll(parentEntry);
						break;
					}
				}
			}
			if (!occured) {
				appended.putAll(childEntry);
			}

			resultList.add(appended);
		}
		return resultList;
	}

	public List<Map<String, Object>> LeftJoin(List<Map<String, Object>> parent, List<Map<String, Object>> child, String parentKey, String childKey) {
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> parentEntry : parent) {
			boolean occured = false;
			Map<String, Object> appended = new HashMap<String, Object>();
			for (Map<String, Object> childEntry : child) {
				if (childEntry.containsKey(childKey) && parentEntry.containsKey(parentKey)) {
					if (childEntry.get(childKey).equals(parentEntry.get(parentKey))) {
						occured = true;
						appended.putAll(childEntry);
						appended.putAll(parentEntry);
						break;
					}
				}
			}
			if (!occured) {
				appended.putAll(parentEntry);
			}

			resultList.add(appended);
		}
		return resultList;
	}

	public List<Map<String, Object>> appendProperKey(List<Map<String, Object>> dataSet, String selectField, String requestField, boolean fetchAllColumn, Map<String, String> selectValues) {

		List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
		if (fetchAllColumn) {
			for (Map<String, Object> data : dataSet) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
				for (Map.Entry<String, String> selectValue : selectValues.entrySet()) {
					for (Map.Entry<String, Object> value : data.entrySet()) {
						if (value.getKey().contains("reaction") && selectValue.getValue().contains("R~RA~")) {
							resultMap.put(selectValue.getKey(), value.getValue());
							break;
						} else if (value.getKey().startsWith(selectValue.getValue())) {
							resultMap.put(selectValue.getKey(), value.getValue());
							break;
						}
					}
				}
				resultSet.add(resultMap);
			}

		} else {
			for (Map<String, Object> data : dataSet) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
				for (Map.Entry<String, String> selectValue : selectValues.entrySet()) {
					for (Map.Entry<String, Object> value : data.entrySet()) {
						if (selectValue.getValue().equalsIgnoreCase(value.getKey())) {
							resultMap.put(selectValue.getKey(), value.getValue());
							break;
						}
					}
				}
				resultSet.add(resultMap);
			}
		}

		return resultSet;
	}

	public void AuthenticateTable(String value, Map<String, Integer> resultSet) {
		int i = 0;
		if (value.contains("title") || value.contains("description") || value.contains("lastModified") || value.contains("thumbnail") || value.contains("createdOn") || value.contains("deleted")
				|| value.contains("itemSequence")) {
			i++;
		}
		resultSet.put("rawTable", i);
		i = 0;
		if (value.contains("timeSpent") || value.contains("views") || value.contains("gooruOId") || value.contains("reaction")) {
			i++;
		}
		resultSet.put("aggregate", i);
		i = 0;
	}

	public OperationResult<ColumnList<String>> read(String keyspace, String columnFamilyName, String key) {
		return cassandraDAO.read(keyspace, columnFamilyName, key);
	}

	public OperationResult<ColumnList<String>> read(String keyspace, String columnFamilyName, String key, Collection<String> columnList) {
		return cassandraDAO.read(keyspace, columnFamilyName, key, columnList);
	}

	public OperationResult<Rows<String, String>> readAll(String keyspace, String columnFamilyName, String columnName) {
		return cassandraDAO.readAll(keyspace, columnFamilyName, columnName);
	}

	public OperationResult<Rows<String, String>> read(String keyspace, String columnFamilyName, String columnName, String columnValue) {
		return cassandraDAO.read(keyspace, columnFamilyName, columnName, columnValue);
	}

	public void markDeletedResource(String startTime, String endTime) {
		if (getBaseService().checkNull(startTime) && getBaseService().checkNull(endTime)) {
			getCassandraDAO().markDeleteStatus(startTime, endTime);
		} else {
			getCassandraDAO().markDeleteStatus();
		}
	}	
}
