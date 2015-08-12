package org.gooru.insights.api.services;

import java.util.Map;

public interface ExcelReportService {

	Map<String, String> getPerformDump(String data,String format,String emailId) throws Exception;
	
	void removeExpiredFiles();
}

