package org.gooru.insights.api.services;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

	protected final Logger LOG = LoggerFactory.getLogger(ClassExportServiceImpl.class);

	public File exportCsv(String classId, String courseId, String unitId, String lessonId, String type, String userId) {

		try {
			List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
			List<String> classMembersList = getClassMembersList(classId, userId);
			List<String> collectionItemsList = getCollectionItems(getLeastId(courseId, unitId, lessonId, type));

			for (String studentId : classMembersList) {
				Map<String, Object> dataMap = getDataMap();
				setUserDetails(dataMap, studentId);
				for (String collectionItemId : collectionItemsList) {
					String title = getContentTitle(collectionItemId);
					String usageRowKey = ServiceUtils.appendTilda(classId, courseId, unitId, lessonId, collectionItemId,
							studentId);
					setDefaultUsage(title, dataMap);
					setUsageData(dataMap, title, usageRowKey, ApiConstants.COLLECTION);
					setUsageData(dataMap, title, usageRowKey, ApiConstants.ASSESSMENT);
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

	private String getLeastId(String courseId, String unitId, String lessonId, String type) {
		String leastId = courseId;
		if (type.equalsIgnoreCase(ApiConstants.COURSE)) {
			leastId = courseId;
		} else if (type.equalsIgnoreCase(ApiConstants.UNIT)) {
			leastId = unitId;
		} else if (type.equalsIgnoreCase(ApiConstants.LESSON)) {
			leastId = lessonId;
		}
		return leastId;
	}

	private List<String> getClassMembersList(String classId, String userId) {
		List<String> classMembersList = null;
		if (StringUtils.isBlank(userId)) {
			classMembersList = getClassMembers(classId);
		} else {
			classMembersList = new ArrayList<String>();
			classMembersList.add(userId);
		}
		return classMembersList;
	}

	private List<String> getCollectionItems(String contentId) {
		List<String> collectionItems = new ArrayList<String>();
		ResultSet collectionItemSet = cqlDAO.getArchievedCollectionItem(contentId);
		for (Row collectionItemRow : collectionItemSet) {
			collectionItems.add(collectionItemRow.getString(ApiConstants.COLUMN1));
		}
		return collectionItems;
	}

	private List<String> getClassMembers(String classId) {
		List<String> classMembersList = new ArrayList<String>();
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

	private void setUsageData(Map<String, Object> dataMap, String title, String rowKey, String collectionType) {
		ResultSet classDataSet = cqlDAO.getArchievedClassData(ServiceUtils.appendTilda(rowKey, collectionType));
		for (Row classDataRow : classDataSet) {
			String columnName = classDataRow.getString(ApiConstants.COLUMN1);
			if (columnName.matches(ApiConstants.COLUMNS_TO_EXPORT)) {
				Object value;
				if (columnName.matches(ApiConstants.BIGINT_COLUMNS)) {
					value = TypeCodec.bigint().deserialize(classDataRow.getBytes(ApiConstants.VALUE),
							cqlDAO.getClusterProtocolVersion());
				} else {
					value = TypeCodec.varchar().deserialize(classDataRow.getBytes(ApiConstants.VALUE),
							cqlDAO.getClusterProtocolVersion());
				}
				dataMap.put(ServiceUtils.appendHyphen(title, collectionType, ExportConstants.csvHeaders(columnName)),
						value);
			}
		}
	}

	private Map<String, Object> getDataMap() {
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put(ExportConstants.FIRST_NAME, "");
		dataMap.put(ExportConstants.LAST_NAME, "");
		return dataMap;
	}

	private void setDefaultUsage(String title, Map<String, Object> dataMap) {
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.COLLECTION, ExportConstants.VIEWS), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.ASSESSMENT, ExportConstants.VIEWS), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.COLLECTION, ExportConstants.TIME_SPENT), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.ASSESSMENT, ExportConstants.TIME_SPENT), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.COLLECTION, ExportConstants.SCORE), 0);
		dataMap.put(ServiceUtils.appendHyphen(title, ApiConstants.ASSESSMENT, ExportConstants.SCORE), 0);
	}
}
