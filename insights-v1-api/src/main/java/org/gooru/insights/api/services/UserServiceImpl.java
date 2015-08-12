package org.gooru.insights.api.services;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.gooru.insights.api.models.RequestParamsDTO;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.gooru.insights.api.spring.exception.BadRequestException;
import org.gooru.insights.api.spring.exception.InsightsServerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class UserServiceImpl implements UserService {

	@Autowired
	private BaseService baseService;

	@Autowired
	private SelectParamsService selectParamsService;

	@Autowired
	private CassandraService cassandraService;

	/*
	 * Reads User Preference data given user_uid
	 */
	public ResponseParamDTO<Map<String, Object>> getPreferenceDataByType(String data, String userUid) throws Exception {

		getBaseService().validateJSON(data);
		RequestParamsDTO requestParamsDTO = this.getBaseService().buildRequestParameters(data);
		ResponseParamDTO<Map<String, Object>> responseObject = new ResponseParamDTO<Map<String, Object>>();
		
		Map<String, Object> resultMap = new HashMap<String, Object>(2);
		Map<String, String> hibernateSelectValues = new HashMap<String, String>();
		requestParamsDTO.setFields(selectParamsService.getUserPreferenceData(requestParamsDTO.getFields(), hibernateSelectValues));

		List<Map<String, Object>> rowData = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> categoryResultSet = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> gradeResultSet = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> subjectResultSet = new ArrayList<Map<String, Object>>();

		if (getBaseService().notNull(userUid)) {

			OperationResult<ColumnList<String>> query = getCassandraService().read("user_preference", userUid);
			for (Column<String> columns : query.getResult()) {
				Map<String, Object> columnMap = new HashMap<String, Object>();
				String columnName = columns.getName();
				if (columnName.contains("preferredCategory")) {
					columnMap.put(columnName, Double.parseDouble(columns.getStringValue()));
					categoryResultSet.add(columnMap);
				} else if (columnName.contains("preferredSubject")) {
					columnMap.put(columnName, Double.parseDouble(columns.getStringValue()));
					subjectResultSet.add(columnMap);

				} else if (columnName.contains("preferredGrade")) {
					columnMap.put(columnName, Double.parseDouble(columns.getStringValue()));
					gradeResultSet.add(columnMap);
				}
			}

			resultMap.put("category", categoryResultSet);
			resultMap.put("grade", gradeResultSet);
			resultMap.put("subject", subjectResultSet);
			rowData.add(resultMap);
			responseObject.setContent(rowData);
		} else {
			throw new BadRequestException("Provided userUid is invalid or is null");
		}
		return responseObject;
	}

	/*
	 * Returns All the rows of Top preference table
	 */
	public ResponseParamDTO<Map<String, Object>> getTopPreferenceList() throws ParseException {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> userList = new ArrayList<Map<String, Object>>();
			OperationResult<Rows<String, String>> data = getCassandraService().readAll("top_preference_vectors", "username");

			for (Row<String, String> rowData : data.getResult()) {
				Map<String, Object> userMap = new HashMap<String, Object>();
				String key = rowData.getKey();
				for (Column<String> column : rowData.getColumns()) {
					userMap.put(key, column.getStringValue());
				}
				userList.add(userMap);
			}
			responseParamDTO.setContent(userList);
			return responseParamDTO;
	}

	/*
	 * Reads User Proficiency data given user_uid and columnName
	 */
	public ResponseParamDTO<Map<Object, Object>> getProficiencyData(String data, String userUid) throws Exception {

		getBaseService().validateJSON(data);
		RequestParamsDTO requestParamsDTO = this.getBaseService().buildRequestParameters(data);

		Map<String, String> hibernateSelectValues = new HashMap<String, String>();
		Map<Object, Object> resultMap = new HashMap<Object, Object>(6);
		List<Map<Object, Object>> resultSet = new ArrayList<Map<Object, Object>>();
		ResponseParamDTO<Map<Object, Object>> responseParamDTO = new ResponseParamDTO<Map<Object, Object>>();
		selectParamsService.getUserProficiencyData(requestParamsDTO.getFields(), hibernateSelectValues);

		try {
			if (getBaseService().notNull(userUid)) {

				if (requestParamsDTO.getFields().contains("subject")) {
					List<Map<Object, Object>> subjectData = new ArrayList<Map<Object, Object>>();
					OperationResult<Rows<String, String>> subjectRows = getCassandraService().read("agg_event_resource_user_subject", "user_uid", userUid);
					for (Row<String, String> subjectRow : subjectRows.getResult()) {
						Integer key = null;
						for (Column<String> column : subjectRow.getColumns()) {
							if (column.getName().contains("subject.")) {
								Map<Object, Object> subjectMap = new HashMap<Object, Object>();
								key = Integer.parseInt(column.getName().replaceAll("subject.", ""));
								subjectMap.put("codeId", key);
								subjectMap.put("vector", Double.parseDouble(column.getStringValue()));
								subjectData.add(subjectMap);
							}
						}
						resultMap.put("subject", new ArrayList<Map<Object, Object>>(new HashSet<Map<Object, Object>>(subjectData)));
					}
				}
				if (requestParamsDTO.getFields().contains("course")) {
					List<Map<Object, Object>> courseData = new ArrayList<Map<Object, Object>>();
					OperationResult<Rows<String, String>> courseRows = getCassandraService().read("agg_event_resource_user_course", "user_uid", userUid);
					for (Row<String, String> courseRow : courseRows.getResult()) {
						Integer key = null;
						for (Column<String> column : courseRow.getColumns()) {
							if (column.getName().contains("course.")) {
								Map<Object, Object> courseMap = new HashMap<Object, Object>();
								key = Integer.parseInt(column.getName().replaceAll("course.", ""));
								courseMap.put("codeId", key);
								courseMap.put("vector", Double.parseDouble(column.getStringValue()));
								courseData.add(courseMap);
							}
						}
						resultMap.put("course", new ArrayList<Map<Object, Object>>(new HashSet<Map<Object, Object>>(courseData)));
					}
				}
				if (requestParamsDTO.getFields().contains("unit")) {
					List<Map<Object, Object>> unitData = new ArrayList<Map<Object, Object>>();
					OperationResult<Rows<String, String>> unitRows = getCassandraService().read("agg_event_resource_user_unit", "user_uid", userUid);
					for (Row<String, String> unitRow : unitRows.getResult()) {
						Integer key = null;
						for (Column<String> column : unitRow.getColumns()) {
							if (column.getName().contains("unit.")) {
								Map<Object, Object> unitMap = new HashMap<Object, Object>();
								key = Integer.parseInt(column.getName().replaceAll("unit.", ""));
								unitMap.put("codeId", key);
								unitMap.put("vector", Double.parseDouble(column.getStringValue()));
								unitData.add(unitMap);
							}
						}
						resultMap.put("unit", new ArrayList<Map<Object, Object>>(new HashSet<Map<Object, Object>>(unitData)));
					}
				}
				if (requestParamsDTO.getFields().contains("topic")) {
					List<Map<Object, Object>> topicData = new ArrayList<Map<Object, Object>>();
					OperationResult<Rows<String, String>> topicRows = getCassandraService().read("agg_event_resource_user_topic", "user_uid", userUid);
					for (Row<String, String> topicRow : topicRows.getResult()) {
						Integer key = null;
						for (Column<String> column : topicRow.getColumns()) {
							if (column.getName().contains("topic.")) {
								Map<Object, Object> topicMap = new HashMap<Object, Object>();
								key = Integer.parseInt(column.getName().replaceAll("topic.", ""));
								topicMap.put("codeId", key);
								topicMap.put("vector", Double.parseDouble(column.getStringValue()));
								topicData.add(topicMap);
							}
						}
						resultMap.put("topic", new ArrayList<Map<Object, Object>>(new HashSet<Map<Object, Object>>(topicData)));
					}
				}
				if (requestParamsDTO.getFields().contains("lesson")) {
					List<Map<Object, Object>> lessonData = new ArrayList<Map<Object, Object>>();
					OperationResult<Rows<String, String>> lessonRows = getCassandraService().read("agg_event_resource_user_lesson", "user_uid", userUid);
					for (Row<String, String> lessonRow : lessonRows.getResult()) {
						Integer key = null;
						for (Column<String> column : lessonRow.getColumns()) {
							if (column.getName().contains("lesson.")) {
								Map<Object, Object> lessonMap = new HashMap<Object, Object>();
								key = Integer.parseInt(column.getName().replaceAll("lesson.", ""));
								lessonMap.put("codeId", key);
								lessonMap.put("vector", Double.parseDouble(column.getStringValue()));
								lessonData.add(lessonMap);
							}
						}
						resultMap.put("lesson", new ArrayList<Map<Object, Object>>(new HashSet<Map<Object, Object>>(lessonData)));
					}
				}
				resultSet.add(resultMap);
				responseParamDTO.setContent(resultSet);
				return responseParamDTO;

			} else {
				throw new BadRequestException("Provided userUid is invalid or is null");
			}
		} catch (Exception e) {
			throw new InsightsServerException(e.getMessage());
		}
	}

	/*
	 * Returns All the rows of Top Proficiency table
	 */
	public ResponseParamDTO<Map<String,Object>> getTopProficiencyList() throws Exception {
		try{
		ResponseParamDTO<Map<String,Object>> responseParamDTO = new ResponseParamDTO<Map<String,Object>>();
			List<Map<String, Object>> userList = new ArrayList<Map<String, Object>>();
			OperationResult<Rows<String, String>> resultRows = getCassandraService().readAll("top_proficiency_vectors", "username");
			for (Row<String, String> rowData : resultRows.getResult()) {
				Map<String, Object> userMap = new HashMap<String, Object>();
				String key = rowData.getKey();
				for (Column<String> column : rowData.getColumns()) {
					userMap.put(key, column.getStringValue());
				}
				userList.add(userMap);
			}
			responseParamDTO.setContent(userList);
			return responseParamDTO;
		}catch (Exception e) {
			throw new InsightsServerException(e.getMessage());
		}
	}

	/*
	 * Get User data from dim_user columnfamily
	 */
	public ResponseParamDTO<Map<String, Object>> getUserData(String data) throws Exception {

		Map<String, String> selectValues = new HashMap<String, String>();
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();

		RequestParamsDTO requestParamsDTO = this.getBaseService().buildRequestParameters(data);
		getBaseService().existsFilter(requestParamsDTO);
		
		requestParamsDTO.setFields(getSelectParamsService().getUser(requestParamsDTO.getFields(), selectValues));
		
		if (getBaseService().notNull(requestParamsDTO.getFilters().getUserUId())) {
			List<Map<String, Object>> userList = new ArrayList<Map<String, Object>>();
			Map<String, Object> userMap = new HashMap<String, Object>();
			OperationResult<ColumnList<String>> resultRow = getCassandraService().read("dim_user", requestParamsDTO.getFilters().getUserUId(),
					getBaseService().convertStringToCollection(requestParamsDTO.getFields()));
			for (Column<String> column : resultRow.getResult()) {
				userMap.put(column.getName(), column.getStringValue());
			}
			userList.add(userMap);
			responseParamDTO.setContent(getBaseService().properName(userList, selectValues));
			return responseParamDTO;
		} else {
			throw new BadRequestException("userUId is null or invalid");
		}
	}

	private BaseService getBaseService() {
		return baseService;
	}

	private SelectParamsService getSelectParamsService() {
		return selectParamsService;
	}

	private CassandraService getCassandraService() {
		return cassandraService;
	}
}
