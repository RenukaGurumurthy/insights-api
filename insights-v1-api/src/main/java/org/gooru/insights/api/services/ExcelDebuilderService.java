package org.gooru.insights.api.services;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.fileupload.FileUploadException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;

public interface ExcelDebuilderService {


	Set<String> getGeneratedReport(HttpServletRequest request,List<Map<String,String>> resultDataReport) throws FileUploadException, IOException, InvalidFormatException;

	Workbook createWorkbook(byte[] data) throws InvalidFormatException, IOException;
	
	String getCellText(Row row, Integer column);
}
