package org.gooru.insights.api.exporters;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import org.gooru.insights.api.constants.ApiConstants;
import org.springframework.stereotype.Component;

@Component
public class CSVFileGenerator {

	@Resource(name = "filePath")
	private Properties filePath;

	private static final String DEFAULT_FILE_NAME = "insights";
	private static final String FILE_REAL_PATH = "insights.file.real.path";
	private static final String FILE_APP_PATH = "insights.file.app.path";
	private static final Pattern FIELDS_TO_TIME_FORMAT =
		Pattern.compile("time_spent|timeSpent|totalTimespent|avgTimespent|timespent|totalTimeSpentInMs|.*Timespent.*");

	public File generateCSVReport(boolean isNewFile,String fileName, List<Map<String, Object>> resultSet) throws
		IOException {

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

	public File generateCSVReport(boolean isNewFile,String fileName, Map<String, Object> resultSet) throws IOException {

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
				}
				// print new line
				stream.println("");
			}
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				Object value = entry.getValue();
				if(FIELDS_TO_TIME_FORMAT.matcher(entry.getKey()).matches()) {
					value = convertMillisecondsToTime(((Number)value).longValue());
				}
				stream.print(appendDQ(value) + ApiConstants.COMMA);
			}
			// print new line
			stream.println(ApiConstants.STRING_EMPTY);
	}

	private PrintStream generatePrintStream(boolean isNewFile,File file) throws FileNotFoundException{
		PrintStream stream;
		if(isNewFile){
			stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file, false)));
		}else{
			stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file, true)));
		}
		return stream;
	}

	private void writeToFile(PrintStream stream){
		stream.flush();
		stream.close();
	}

	private String setFilePath(String file){

		String fileName = filePath.getProperty(FILE_REAL_PATH);
		if(file != null && (!file.isEmpty())){
			fileName += file;
		}else{
			fileName +=DEFAULT_FILE_NAME;
		}
		return fileName+ApiConstants.CSV_EXT;
	}

	public String getFilePath(String file){

		String fileName = filePath.getProperty(FILE_APP_PATH);
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

	private String convertMillisecondsToTime(long millis) {
		return String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
			    TimeUnit.MILLISECONDS.toMinutes(millis) % TimeUnit.HOURS.toMinutes(1),
			    TimeUnit.MILLISECONDS.toSeconds(millis) % TimeUnit.MINUTES.toSeconds(1));
	}
}
