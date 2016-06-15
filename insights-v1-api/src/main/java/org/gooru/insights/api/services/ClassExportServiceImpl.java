package org.gooru.insights.api.services;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ExportConstants;
import org.gooru.insights.api.daos.CqlCassandraDao;
import org.gooru.insights.api.exporters.CSVFileGenerator;
import org.gooru.insights.api.utils.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;

@Service
public class ClassExportServiceImpl implements ClassExportService {

	@Autowired
	CSVFileGenerator csvFileGenerator;

	@Autowired
	private CqlCassandraDao cqlDAO;

	private final Logger LOG = LoggerFactory.getLogger(ClassExportServiceImpl.class);

	@Override
	public File exportCsv(String classId, String courseId, String unitId, String lessonId, String collectionId,
			String type, String userId) {
		try {

			List<Map<String, Object>> dataList = new ArrayList<>();
			List<String> classMembersList = getClassMembersList(classId, userId);
			List<String> collectionItemsList = getCollectionItems(getLeastId(courseId, unitId, lessonId, collectionId, type));

			for (String studentId : classMembersList) {
				Map<String, Object> dataMap = getDataMap();
				setUserDetails(dataMap, studentId);
				ResultSet usageDataSet = null;
				if (type.equalsIgnoreCase(ApiConstants.COLLECTION)) {
					String rowKey = getSessionId(ServiceUtils.appendTilda(ApiConstants.RS, classId, courseId, unitId,
							lessonId, collectionId, studentId));
					usageDataSet = cqlDAO.getArchievedSessionData(rowKey);
					LOG.info("rowKey: " + rowKey);
				}
				for (String collectionItemId : collectionItemsList) {
					String title = getContentTitle(collectionItemId);
					if (type.equalsIgnoreCase(ApiConstants.COLLECTION)) {
						setDefaultResourceUsage(title, dataMap);
						if(usageDataSet != null){
							processResultSet(usageDataSet, true, ApiConstants.RESOURCE_COLUMNS_TO_EXPORT, dataMap, title, null);
						}
					} else {
						String usageRowKey = ServiceUtils.appendTilda(classId, courseId, unitId, lessonId, collectionItemId,
								studentId);
						LOG.info("usageRowKey: " + usageRowKey);
						setDefaultUsage(title, dataMap);
						setUsageData(dataMap, title, usageRowKey, ApiConstants.COLLECTION);
						setUsageData(dataMap, title, usageRowKey, ApiConstants.ASSESSMENT);
					}
				}
				dataList.add(dataMap);
			}

			return csvFileGenerator.generateCSVReport(true, ServiceUtils.appendHyphen(type, ApiConstants.DATA),
					dataList);

		} catch (Exception e) {
			LOG.error("Exception while generating CSV", e);
		}
		return null;
	}

	private String getLeastId(String courseId, String unitId, String lessonId, String collectionId, String type) {
		String leastId = courseId;
		if (type.equalsIgnoreCase(ApiConstants.COURSE)) {
			leastId = courseId;
		} else if (type.equalsIgnoreCase(ApiConstants.UNIT)) {
			leastId = unitId;
		} else if (type.equalsIgnoreCase(ApiConstants.LESSON)) {
			leastId = lessonId;
		} else if (type.equalsIgnoreCase(ApiConstants.COLLECTION)) {
			leastId = collectionId;
		}
		return leastId;
	}

	private List<String> getClassMembersList(String classId, String userId) {
		List<String> classMembersList;
		if (StringUtils.isBlank(userId)) {
			classMembersList = getClassMembers(classId);
		} else {
			classMembersList = new ArrayList<>();
			classMembersList.add(userId);
		}
		return classMembersList;
	}

	private List<String> getCollectionItems(String contentId) {
		List<String> collectionItems = new ArrayList<>();
		ResultSet collectionItemSet = cqlDAO.getArchievedCollectionItem(contentId);
		for (Row collectionItemRow : collectionItemSet) {
			collectionItems.add(collectionItemRow.getString(ApiConstants.COLUMN1));
		}
		return collectionItems;
	}

	private List<String> getClassMembers(String classId) {
		List<String> classMembersList = new ArrayList<>();
		ResultSet classMemberSet = cqlDAO.getArchievedClassMembers(classId);
		for (Row collectionItemRow : classMemberSet) {
			classMembersList.add(collectionItemRow.getString(ApiConstants.COLUMN1));
		}
		return classMembersList;
	}

	private void setUserDetails(Map<String, Object> dataMap, String userId) {
		ResultSet userDetailSet = cqlDAO.getArchievedUserDetails(userId);
		for (Row userDetailRow : userDetailSet) {
			dataMap.put(ExportConstants.FIRST_NAME, userDetailRow.getString("firstname"));
			dataMap.put(ExportConstants.LAST_NAME, userDetailRow.getString("lastname"));
		}
	}

	private String getContentTitle(String contentId) {
		String title = "";
		ResultSet contentDetails = cqlDAO.getArchievedContentTitle(contentId);
		for (Row contentDetailRow : contentDetails) {
			title = contentDetailRow.getString(ApiConstants.TITLE);
		}
		return title;
	}

	private String getSessionId(String rowKey) {
		String sessionId = null;
		ResultSet sessionIdSet = cqlDAO.getArchievedCollectionRecentSessionId(rowKey);
		for (Row sessionIdRow : sessionIdSet) {
			sessionId = TypeCodec.varchar().deserialize(sessionIdRow.getBytes(ApiConstants.VALUE),
					cqlDAO.getClusterProtocolVersion());
		}
		return sessionId;
	}

	private void setUsageData(Map<String, Object> dataMap, String title, String rowKey, String collectionType) {
		String columnNames = ApiConstants.COLUMNS_TO_EXPORT;
		boolean splitColumnName = false;
		ResultSet usageDataSet = cqlDAO.getArchievedClassData(ServiceUtils.appendTilda(rowKey, collectionType));
		processResultSet(usageDataSet, splitColumnName, columnNames, dataMap, title, collectionType);
	}

	private void processResultSet(ResultSet usageDataSet, boolean splitColumnName, String columnNames,Map<String, Object> dataMap, String title, String collectionType){
		for (Row usageDataRow : usageDataSet) {
			String dbColumnName = usageDataRow.getString(ApiConstants.COLUMN1);
			if (dbColumnName.matches(columnNames)) {
			String columnName = splitColumnName ? dbColumnName.split(ApiConstants.TILDA)[1] : dbColumnName;
				Object value ;
				if(ApiConstants.BIGINT_COLUMNS_PATTERN.matcher(columnName).matches()){
				value = TypeCodec.bigint().deserialize(usageDataRow.getBytes(ApiConstants.VALUE),
						cqlDAO.getClusterProtocolVersion());
				}else{
					value = TypeCodec.varchar().deserialize(usageDataRow.getBytes(ApiConstants.VALUE),
							cqlDAO.getClusterProtocolVersion());
				}
				dataMap.put(ServiceUtils.appendHyphen(title, collectionType, ExportConstants.csvHeaders(columnName)), value);
			}
		}
	}
	private Map<String, Object> getDataMap() {
		Map<String, Object> dataMap = new LinkedHashMap<>();
		dataMap.put(ExportConstants.FIRST_NAME, "");
		dataMap.put(ExportConstants.LAST_NAME, "");
		return dataMap;
	}

	private void setDefaultUsage(String title, Map<String, Object> dataMap) {
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.COLLECTION, ExportConstants.VIEWS), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.ASSESSMENT, ExportConstants.VIEWS), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.COLLECTION, ExportConstants.TIME_SPENT), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.ASSESSMENT, ExportConstants.TIME_SPENT), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.COLLECTION, ExportConstants.SCORE_IN_PERCENTAGE), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.ASSESSMENT, ExportConstants.SCORE_IN_PERCENTAGE), 0);
	}

	private void setDefaultResourceUsage(String title, Map<String, Object> dataMap) {
		dataMap.put(ServiceUtils.appendHyphen(title, ExportConstants.VIEWS), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ExportConstants.TIME_SPENT), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ExportConstants.SCORE_IN_PERCENTAGE), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ExportConstants.ANSWER_STATUS), ApiConstants.NA);
	}
}
