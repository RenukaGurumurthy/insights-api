package org.gooru.insights.api.exporters;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Resource;

import org.gooru.insights.api.constants.ApiConstants;
import org.springframework.stereotype.Component;

@Component
public class CSVFileGenerator {
	
	@Resource(name = "filePath")
	private Properties filePath;
	
	public static final String DEFAULT_FILE_NAME = "insights";
	public static final String FILE_REAL_PATH = "insights.file.real.path";
	public static final String FILE_APP_PATH = "insights.file.app.path";
	
	public File generateCSVReport(boolean isNewFile,String fileName, List<Map<String, Object>> resultSet) throws ParseException, IOException {

		boolean headerColumns = false;
		File csvfile = new File(setFilePath(fileName));
		PrintStream stream = generatePrintStream(isNewFile,csvfile);
		for (Map<String, Object> map : resultSet) {
			writeToStream(map,stream,headerColumns);
			headerColumns = true;
		}
		writeToFile(stream);
		return csvfile;
	}

	public File generateCSVReport(boolean isNewFile,String fileName, Map<String, Object> resultSet) throws ParseException, IOException {

		boolean headerColumns = false;
		File csvfile = new File(setFilePath(fileName));
		PrintStream stream = generatePrintStream(isNewFile,csvfile);
		writeToStream(resultSet,stream,headerColumns);
		writeToFile(stream);
		return csvfile;
	}
	
	public void includeEmptyLine(boolean isNewFile,String fileName, int lineCount) throws FileNotFoundException{
		
		File csvfile = new File(setFilePath(fileName));
		PrintStream stream = generatePrintStream(isNewFile,csvfile);
		for(int i =0; i < lineCount;i++){
			stream.println(ApiConstants.STRING_EMPTY);
		}
		writeToFile(stream);
	}
	
	private Object appendDQ(Object key) {
	    return ApiConstants.DOUBLE_QUOTES + key + ApiConstants.DOUBLE_QUOTES;
	}
	
	private void writeToStream(Map<String,Object> map,PrintStream stream,boolean headerColumns){
			if (!headerColumns) {
				for (Map.Entry<String, Object> entry : map.entrySet()) {
					stream.print(appendDQ(entry.getKey()) + ApiConstants.COMMA);
					headerColumns = true;
				}
				// print new line
				stream.println("");
			}
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				stream.print(appendDQ(entry.getValue()) + ApiConstants.COMMA);
			}
			// print new line
			stream.println(ApiConstants.STRING_EMPTY);
	}

	private PrintStream generatePrintStream(boolean isNewFile,File file) throws FileNotFoundException{
		PrintStream stream = null;
		File csvfile = file;
		if(isNewFile){
			stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(csvfile, false)));
		}else{
			stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(csvfile, true)));
		}
		return stream;
	}

	private void writeToFile(PrintStream stream){
		stream.flush();
		stream.close();
	}
	
	public String setFilePath(String file){
		
		String fileName = this.getFilePath().getProperty(FILE_REAL_PATH);
		if(file != null && (!file.isEmpty())){
			fileName += file;
		}else{
			fileName +=DEFAULT_FILE_NAME;
		}
		return fileName;
	}

	public String getFilePath(String file){
		
		String fileName = this.getFilePath().getProperty(FILE_APP_PATH);
		if(file != null && (!file.isEmpty())){
			fileName += file;
		}else{
			fileName +=DEFAULT_FILE_NAME;
		}
		return fileName;
	}

	public Properties getFilePath() {
		return filePath;
	}

	public void setFilePath(Properties filePath) {
		this.filePath = filePath;
	}
	
}
