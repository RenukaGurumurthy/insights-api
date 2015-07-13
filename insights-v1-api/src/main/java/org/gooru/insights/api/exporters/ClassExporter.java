package org.gooru.insights.api.exporters;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.controllers.BaseController;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Component
@RequestMapping(value="/export")
public class ClassExporter extends BaseController{
	
	@Autowired
	ClassExporterProcessor classExporterProcessor;

	@RequestMapping(value="/class/{classId}/course/{courseId}/unit/{unitId}/lesson/{lessonId}/{collectionType}/{collectionId}/users", method = {RequestMethod.GET,RequestMethod.POST})
	public void exportClassUserUsageReport(HttpServletRequest request,
			@PathVariable(value ="classId") String classId,@PathVariable(value ="courseId") String courseId,
			@PathVariable(value ="unitId") String unitId,@PathVariable(value ="lessonId") String lessonId,
			@PathVariable(value ="collectionType") String collectionType,@PathVariable(value ="collectionId") String collectionId,HttpServletResponse response) throws JSONException, ParseException, IOException{
		
		File file = getClassExporterProcessor().exportClassUserUsageReport(getTraceId(request),classId, courseId, unitId, lessonId, collectionType, collectionId);
		generateCSVOutput(response,file);
	}

	public ClassExporterProcessor getClassExporterProcessor() {
		return classExporterProcessor;
	}

	public void setClassExporterProcessor(
			ClassExporterProcessor classExporterProcessor) {
		this.classExporterProcessor = classExporterProcessor;
	}
}
