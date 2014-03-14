/*******************************************************************************
 * UserServiceImpl.java
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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.insights.api.daos.CassandraDAO;
import org.gooru.insights.api.models.InsightsConstant.checkFields;
import org.gooru.insights.api.models.RequestParamsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Transactional
@Service
public class UserServiceImpl implements UserService {

	@Autowired
	private BaseService baseService;

	@Autowired
	private SelectParamsService selectParamsService;

	@Autowired
	private CassandraService cassandraService;

	@Autowired
	private CassandraDAO cassandraDAO;
	
	String gooruSearchKeyspaceCli = "goorusearch.keyspace";
	String logKeyspaceCli = "log.keyspace";

	/*
	 * Reads User Preference data given user_uid
	 */
	public List<Map<String, Object>> getPreferenceDataByType(String databaseType, String data, String userUid, List<Map<String, String>> errorData) throws ParseException {
		Map<String, String> selectValues = new HashMap<String, String>();
		if (getBaseService().checkData(data, errorData)) {
			RequestParamsDTO requestParamsDTO = this.getBaseService().buildRequestParameters(data);
			if (databaseType.equalsIgnoreCase("cassandra")) {
				if (getBaseService().validate(data, requestParamsDTO, errorData)) {
					if (getBaseService().checkNull(requestParamsDTO.getFields())) {
						Map<String, String> hibernateSelectValues = new HashMap<String, String>();
						hibernateSelectValues = selectParamsService.getUserPreferenceData(requestParamsDTO.getFields(), selectValues);
						List<Map<String, Object>> rowData = new ArrayList<Map<String, Object>>();
						Map<String, String> errorMessage = new HashMap<String, String>();
						String selectFields = hibernateSelectValues.get("select");
						String InvalidFields = hibernateSelectValues.get("InValidParameters");
						String requestFields = hibernateSelectValues.get("requestedValues");

						List<Map<String, Object>> categoryResultSet = new ArrayList<Map<String, Object>>();
						List<Map<String, Object>> gradeResultSet = new ArrayList<Map<String, Object>>();
						List<Map<String, Object>> subjectResultSet = new ArrayList<Map<String, Object>>();
						Map<String, Object> resultMap = new HashMap<String, Object>(2);
						if (getBaseService().checkNull(InvalidFields)) {
							errorMessage.put("checkDocument", InvalidFields);
						}
						if (getBaseService().checkNull(userUid)) {

							OperationResult<ColumnList<String>> query = getCassandraService().read(gooruSearchKeyspaceCli, "user_preference", userUid);

							OperationResult<ColumnList<String>> singleRecord = getCassandraService().read(gooruSearchKeyspaceCli, "user_preference", userUid,
									this.convertStringToCollection(selectFields));

							for (Column<String> columns : query.getResult()) {
								Map<String, Object> columnMap = new HashMap<String, Object>();
								String columnName = columns.getName();
								if (columnName != null) {
									if (columnName.contains("preferredCategory") && requestFields.contains("category")) {
										// columnMap.put(columnName.replaceAll("preferredCategory.",""),
										// columns.getStringValue());
										columnMap.put(columnName, Double.parseDouble(columns.getStringValue()));
										categoryResultSet.add(columnMap);

									} else if (columnName.contains("preferredSubject") && requestFields.contains("subject")) {
										// columnMap.put(columnName.replaceAll("preferredSubject.",""),
										// columns.getStringValue());
										columnMap.put(columnName, Double.parseDouble(columns.getStringValue()));
										subjectResultSet.add(columnMap);

									}
								}
							}
							for (Column<String> columns : singleRecord.getResult()) {
								Map<String, Object> columnMap = new HashMap<String, Object>();
								String columnName = columns.getName();
								if (columnName.contains("preferredGrade") && requestFields.contains("grade")) {
									// columnMap.put(columnName.replaceAll("preferredGrade.",""),
									// columns.getStringValue());
									columnMap.put(columnName, Double.parseDouble(columns.getStringValue()));
									gradeResultSet.add(columnMap);

								}
							}
							resultMap.put("category", categoryResultSet);
							resultMap.put("grade", gradeResultSet);
							resultMap.put("subject", subjectResultSet);
							rowData.add(resultMap);
							return rowData;
						} else {
							errorMessage.put("Invalid Input", "Provided userUid is invalid or is null");
							return rowData;
						}

						// return
						// getCassandraService().getUserPreferredData(requestParamsDTO,
						// selectParamsService.getUserPreferenceData(requestParamsDTO.getFields(),
						// selectValues), userUid, errorData);
					} else {
						Map<String, String> error = new HashMap<String, String>();
						error.put(checkFields.KEY.getFields(), checkFields.VALUE.getFields());
						errorData.add(error);
					}
				}
			}
		}
		return null;
	}

	/*
	 * Returns All the rows of Top preference table
	 */
	public List<Map<String, Object>> getTopPreferenceList(String databaseType, List<Map<String, String>> errorData) throws ParseException {
		if (databaseType.equalsIgnoreCase("cassandra")) {
			// return getCassandraService().getTopPreference("username",
			// errorData);
			List<Map<String, Object>> userList = new ArrayList<Map<String, Object>>();
			OperationResult<Rows<String, String>> data = cassandraDAO.readAll(gooruSearchKeyspaceCli, "top_preference_vectors", "username");

			for (Row<String, String> rowData : data.getResult()) {
				Map<String, Object> userMap = new HashMap<String, Object>();
				String key = rowData.getKey();
				for (Column<String> column : rowData.getColumns()) {
					userMap.put(key, column.getStringValue());
				}
				userList.add(userMap);
			}

			return userList;
		}
		return null;
	}

	/*
	 * Reads User Proficiency data given user_uid and columnName
	 */
	public List<Map<Object, Object>> getProficiencyData(String databaseType, String data, String userUid, List<Map<String, String>> errorData) throws ParseException {
		Map<String, String> selectValues = new HashMap<String, String>();
		if (getBaseService().checkData(data, errorData)) {
			RequestParamsDTO requestParamsDTO = this.getBaseService().buildRequestParameters(data);
			if (databaseType.equalsIgnoreCase("cassandra")) {
				if (getBaseService().validate(data, requestParamsDTO, errorData)) {
					if (getBaseService().checkNull(requestParamsDTO.getFields())) {

						Map<String, String> hibernateSelectValues = selectParamsService.getUserProficiencyData(requestParamsDTO.getFields(), selectValues);
						Map<String, String> errorMessage = new HashMap<String, String>();
						String selectFields = hibernateSelectValues.get("select");
						String InvalidFields = hibernateSelectValues.get("InValidParameters");
						String requestFields = hibernateSelectValues.get("requestedValues");

						List<Map<Object, Object>> subjectData = new ArrayList<Map<Object, Object>>();
						List<Map<Object, Object>> courseData = new ArrayList<Map<Object, Object>>();
						List<Map<Object, Object>> unitData = new ArrayList<Map<Object, Object>>();
						List<Map<Object, Object>> topicData = new ArrayList<Map<Object, Object>>();
						List<Map<Object, Object>> lessonData = new ArrayList<Map<Object, Object>>();
						List<Map<Object, Object>> resultSet = new ArrayList<Map<Object, Object>>();
						Map<Object, Object> resultMap = new HashMap<Object, Object>(6);

						if (getBaseService().checkNull(InvalidFields)) {
							errorMessage.put("checkDocument", InvalidFields);
						}
						if (getBaseService().checkNull(userUid)) {

							OperationResult<Rows<String, String>> resultRow = getCassandraService().read(gooruSearchKeyspaceCli, "user_proficiency", "user_uid", userUid);
							for (Row<String, String> row : resultRow.getResult()) {

								Integer key = null;
								for (Column<String> column : row.getColumns()) {
									if (column.getName().contains("subject") && requestFields.contains("subject")) {
										Map<Object, Object> subjectMap = new HashMap<Object, Object>();
										key = Integer.parseInt(column.getName().replaceAll("subject.", ""));
										subjectMap.put(key, Double.parseDouble(column.getStringValue()));
										subjectData.add(subjectMap);
									} else if (column.getName().contains("course") && requestFields.contains("course")) {
										Map<Object, Object> courseMap = new HashMap<Object, Object>();
										key = Integer.parseInt(column.getName().replaceAll("course.", ""));
										courseMap.put(key, Double.parseDouble(column.getStringValue()));
										courseData.add(courseMap);
									} else if (column.getName().contains("unit") && requestFields.contains("unit")) {
										Map<Object, Object> unitMap = new HashMap<Object, Object>();
										key = Integer.parseInt(column.getName().replaceAll("unit.", ""));
										unitMap.put(key, Double.parseDouble(column.getStringValue()));
										unitData.add(unitMap);
									} else if (column.getName().contains("topic") && requestFields.contains("topic")) {
										Map<Object, Object> topicMap = new HashMap<Object, Object>();
										key = Integer.parseInt(column.getName().replaceAll("topic.", ""));
										topicMap.put(key, Double.parseDouble(column.getStringValue()));
										topicData.add(topicMap);
									} else if (column.getName().contains("lesson") && requestFields.contains("lesson")) {
										Map<Object, Object> lessonMap = new HashMap<Object, Object>();
										key = Integer.parseInt(column.getName().replaceAll("lesson.", ""));
										lessonMap.put(key, Double.parseDouble(column.getStringValue()));
										lessonData.add(lessonMap);
									}
								}
								resultMap.put("subject", subjectData);
								resultMap.put("course", courseData);
								resultMap.put("unit", unitData);
								resultMap.put("topic", topicData);
								resultMap.put("lesson", lessonData);
							}
							resultSet.add(resultMap);
							return resultSet;

						} else {

							errorMessage.put("Invalid Input", "Provided userUid is invalid or is null");
							return resultSet;
						}
						// return
						// getCassandraService().getUserProficiencyData(requestParamsDTO,
						// selectParamsService.getUserProficiencyData(requestParamsDTO.getFields(),
						// selectValues), userUid, errorData);
					} else {
						Map<String, String> error = new HashMap<String, String>();
						error.put(checkFields.KEY.getFields(), checkFields.VALUE.getFields());
						errorData.add(error);
					}
				}
			}
		}
		return null;
	}

	/*
	 * Returns All the rows of Top Proficiency table
	 */
	public List<Map<String, Object>> getTopProficiencyList(String databaseType, List<Map<String, String>> errorData) throws ParseException {
		if (databaseType.equalsIgnoreCase("cassandra")) {

			List<Map<String, Object>> userList = new ArrayList<Map<String, Object>>();
			OperationResult<Rows<String, String>> resultRows = getCassandraService().readAll(gooruSearchKeyspaceCli, "top_proficiency_vectors", "username");
			for (Row<String, String> rowData : resultRows.getResult()) {
				Map<String, Object> userMap = new HashMap<String, Object>();
				String key = rowData.getKey();
				for (Column<String> column : rowData.getColumns()) {
					userMap.put(key, column.getStringValue());
				}
				userList.add(userMap);
			}

			return userList;
		}
		return null;
	}

	/*
	 * Get UserUid from dim_user table given username
	 */
	public List<Map<String, Object>> getUserUid(String databaseType, String userName, List<Map<String, String>> errorData) throws ParseException {
		if (databaseType.equalsIgnoreCase("cassandra")) {
			if (getBaseService().checkNull(userName)) {
				// return getCassandraService().getUserData(userName,
				// errorData);
				String userUid = null;
				List<Map<String, Object>> userList = new ArrayList<Map<String, Object>>();

				OperationResult<Rows<String, String>> resultRow = getCassandraService().read(logKeyspaceCli, "dim_user", "username", userName);

				for (Row<String, String> userId : resultRow.getResult()) {
					Map<String, Object> columnMap = new HashMap<String, Object>();
					for (Column<String> column : userId.getColumns()) {
						if (column.getName().equalsIgnoreCase("gooru_uid")) {
							userUid = column.getStringValue();
						}
					}
					columnMap.put("userUid", userUid);
					columnMap.put("username", userName);

					userList.add(columnMap);
				}
				return userList;
			} else {
				Map<String, String> error = new HashMap<String, String>();
				error.put(checkFields.KEY.getFields(), checkFields.VALUE.getFields());
				errorData.add(error);
			}

		}
		return null;
	}

	private Collection<String> convertStringToCollection(String data) {
		Collection<String> collection = new ArrayList<String>();
		for (String value : data.split(",")) {
			collection.add(value);
		}
		return collection;
	}
	
	public void setBaseService(BaseService baseService) {
		this.baseService = baseService;
	}

	public BaseService getBaseService() {
		return baseService;
	}

	public void setSelectParamsService(SelectParamsService selectParamsService) {
		this.selectParamsService = selectParamsService;
	}

	public SelectParamsService getSelectParamsService() {
		return selectParamsService;
	}

	public void setCassandraService(CassandraService cassandraService) {
		this.cassandraService = cassandraService;
	}

	public CassandraService getCassandraService() {
		return cassandraService;
	}

}
