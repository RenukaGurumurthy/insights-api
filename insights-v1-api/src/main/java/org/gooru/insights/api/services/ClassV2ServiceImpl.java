package org.gooru.insights.api.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.InsightsConstant;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Service
public class ClassV2ServiceImpl implements ClassV2Service, InsightsConstant{
	
	@Autowired
	private CassandraV2Service cassandraService;

	private CassandraV2Service getCassandraService() {
		return cassandraService;
	}
	
	@Autowired
	private BaseService baseService;

	private BaseService getBaseService() {
		return baseService;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getUserCurrentLocationInLesson(String userUid, String classId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		ColumnList<String> resultColumns = getCassandraService().getUserCurrentLocation(ColumnFamily.STUDENT_LOCATION.getColumnFamily(), userUid, classId);
		if (resultColumns != null && resultColumns.size() > 0) {
			Map<String, Object> dataAsMap = new HashMap<String, Object>();
			dataAsMap.put(ApiConstants.CLASS_GOORU_ID, resultColumns.getStringValue("class_uid", null));
			dataAsMap.put("courseId", resultColumns.getStringValue("course_uid", null));
			dataAsMap.put("unitId", resultColumns.getStringValue("unit_uid", null));
			dataAsMap.put("lessonId", resultColumns.getStringValue("lesson_uid", null));
			dataAsMap.put("gooruOid", resultColumns.getStringValue("collection_uid", null));
			dataMapAsList.add(dataAsMap);
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}
	
	@Override
	public ResponseParamDTO<Map<String, Object>> getUserPeers(String classId, String courseId, String unitId, String lessonId) {
		ResponseParamDTO<Map<String, Object>> responseParamDTO = new ResponseParamDTO<Map<String, Object>>();
		List<Map<String, Object>> dataMapAsList = new ArrayList<Map<String, Object>>();
		String rowKey = getBaseService().appendTilda(classId, courseId, unitId, lessonId);
		Rows<String, String> resultRows = getCassandraService().readColumnsWithKey(ColumnFamily.CLASS_ACTIVITY_PEER_COUNTS.getColumnFamily(), rowKey);
		if (resultRows != null && resultRows.size() > 0) {
			for(Row<String, String> resultRow : resultRows) {
				Map<String, Object> dataAsMap = new HashMap<String, Object>();
				ColumnList<String> columnList = resultRow.getColumns();
				dataAsMap.put(ApiConstants.GOORUOID, columnList.getStringValue(ApiConstants._LEAF_GOORU_OID, null));
				dataAsMap.put(ApiConstants.ACTIVE_PEER_COUNT, columnList.getLongValue(ApiConstants._ACTIVE_GOORU_OID, 0L));
				dataAsMap.put(ApiConstants.LEFT_PEER_COUNT, columnList.getLongValue(ApiConstants._LEFT_PEER_COUNT, 0L));
				dataMapAsList.add(dataAsMap);
			}
		}
		responseParamDTO.setContent(dataMapAsList);
		return responseParamDTO;
	}
	
}
