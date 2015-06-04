package org.gooru.insights.api.services;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.stereotype.Service;

@Service
public class ExcelDebuilderServiceImpl implements ExcelDebuilderService{

	public Set<String> getGeneratedReport(HttpServletRequest request, List<Map<String, String>> resultDataReport) throws FileUploadException, IOException, InvalidFormatException {

		Set<String> gooruOIds = new TreeSet<String>();
		Map<String, String> dataReport = new HashMap<String, String>();
		if (ServletFileUpload.isMultipartContent(request)) {

			ServletFileUpload upload = new ServletFileUpload();
			FileItemIterator iter = upload.getItemIterator(request);
			while (iter.hasNext()) {

				FileItemStream item = iter.next();
				InputStream stream = item.openStream();

				int len;
				byte[] buffer = new byte[8192];
				ByteArrayOutputStream bout = new ByteArrayOutputStream();
				while ((len = stream.read(buffer, 0, buffer.length)) != -1) {
					bout.write(buffer, 0, len);
				}
				String fieldName = item.getName();
				byte[] data = bout.toByteArray();

				this.FetchExcelReport(data, fieldName, gooruOIds, resultDataReport);
			}
		} else {
			dataReport.put("CheckRequestFormat", "please attach a file to process");
			resultDataReport.add(dataReport);
		}
		return gooruOIds;
	}

	public void FetchExcelReport(byte[] data, String fieldName, Set<String> gooruOIds, List<Map<String, String>> resultDataReport) throws InvalidFormatException, IOException {
		Map<String, String> resultSet = new HashMap<String, String>();
		Workbook workbook = this.createWorkbook(data);
		Sheet sheet = null;
		int numSheets = workbook.getNumberOfSheets();
		Integer gooruOId;

		for (int i = 0; i < numSheets; i++) {

			// Get a reference to a sheet and check to see if it contains
			// any rows.
			sheet = workbook.getSheetAt(i);

			int startIndex = 0;
			gooruOId = null;

			boolean validPage = false;
			while (!validPage && startIndex < 5) {
				Row headerRow = sheet.getRow(startIndex++);
				if (checkNull(getCell("GooruOId", headerRow))) {
					gooruOId = getCell("GooruOId", headerRow);
					validPage = true;
				}

			}
			if (gooruOId != null) {
			} else {

				resultSet.put("CheckSheet", "please provide the GooruOId field in " + fieldName + "");
				resultDataReport.add(resultSet);
				validPage = false;
			}
			if (validPage) {

				if (sheet.getPhysicalNumberOfRows() > 0) {
					int lastRowNum = sheet.getLastRowNum();
					for (int rowIndex = startIndex; rowIndex <= lastRowNum; rowIndex++) {
						Row row = sheet.getRow(rowIndex);
						if (checkNull(getCellText(row, gooruOId))) {
							this.addGooruOId(getCellText(row, gooruOId), gooruOIds);
						}

					}
					if (gooruOIds.isEmpty()) {
						resultSet.put("CheckSheetRecord(" + sheet.getSheetName() + ")", "please enter atleast one gooruOId in gooruOId field in " + fieldName + "");
						resultDataReport.add(resultSet);
					}

				}
			}

		}
	}

	private Set<String> addGooruOId(String gooruOId, Set<String> data) {
		data.add(gooruOId);
		return data;

	}
	
	private boolean checkNull(String data) {

		if (data != null && (!data.isEmpty())) {
			return true;
		}
		return false;
	}
	
	private boolean checkNull(Integer data) {

		if (data != null && (data.SIZE > 0)) {

			return true;
		}
		return false;
	}

	private Integer getCell(String cellName, Row row) {

		for (int cell = 0; cell <= row.getLastCellNum(); cell++) {
			if (cellName.equalsIgnoreCase(row.getCell(cell).toString().trim()) || "GooruId".equalsIgnoreCase(row.getCell(cell).toString().trim())) {
				return cell;
			}
		}
		return null;
	}

	public String getCellText(Row row, Integer column) {
		if (row != null && column != null) {
			String text = row.getCell(column).toString().trim();
			return (text.equals("")) ? null : text;
		}
		return null;
	}

	public Workbook createWorkbook(byte[] data) throws InvalidFormatException, IOException{

		Workbook workbook;
		FormulaEvaluator evaluator;
		DataFormatter formatter;
		ByteArrayInputStream bais = null;
		try{
		
		bais = new ByteArrayInputStream(data);
		workbook = WorkbookFactory.create(bais);
		evaluator = workbook.getCreationHelper().createFormulaEvaluator();
		formatter = new DataFormatter();
		
		}finally {
			if (bais != null) {
				bais.close();
			}
		}
		return workbook;
	}
}
