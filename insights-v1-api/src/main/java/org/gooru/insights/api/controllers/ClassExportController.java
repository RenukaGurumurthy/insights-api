package org.gooru.insights.api.controllers;

import java.io.File;
import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.services.ClassExportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value="/v2/")
public class ClassExportController extends BaseController{

	@Autowired
	public ClassExportService classExportService;
	
	@RequestMapping(value="class/{classId}/course/{courseId}/units/export", method = {RequestMethod.GET})
	public void exportClassUnits(HttpServletRequest request,
			@PathVariable(value ="classId") String classId,
			@PathVariable(value="courseId") String courseId,@RequestParam(value = "userId", required = false) String userId, HttpServletResponse response) throws IOException{
		File file = classExportService.exportCsv(classId, courseId,null, null, null, ApiConstants.COURSE, userId);
		generateCSVOutput(response,file);
	}
	
	@RequestMapping(value="class/{classId}/course/{courseId}/unit/{unitId}/lessons/export", method = {RequestMethod.GET})
	public void exportClassLessons(HttpServletRequest request,
			@PathVariable(value ="classId") String classId,
			@PathVariable(value="courseId") String courseId,@PathVariable(value="unitId") String unitId,@RequestParam(value = "userId", required = false) String userId, HttpServletResponse response) throws IOException{
		File file = classExportService.exportCsv(classId, courseId, unitId, null,null, ApiConstants.UNIT, userId);
		generateCSVOutput(response,file);
	}
	
	@RequestMapping(value="class/{classId}/course/{courseId}/unit/{unitId}/lesson/{lessonId}/items/export", method = {RequestMethod.GET})
	public void exportClassCollections(HttpServletRequest request,
			@PathVariable(value ="classId") String classId,
			@PathVariable(value="courseId") String courseId,@PathVariable(value="unitId") String unitId,@PathVariable(value="lessonId") String lessonId,@RequestParam(value = "userId", required = false) String userId, HttpServletResponse response) throws IOException{
		File file = classExportService.exportCsv(classId, courseId, unitId, lessonId, null, ApiConstants.LESSON, userId);
		generateCSVOutput(response,file);
	}
	
	@RequestMapping(value="class/{classId}/course/{courseId}/unit/{unitId}/lesson/{lessonId}/{collectionType}/{collectionId}/items/export", method = {RequestMethod.GET})
	public void exportResource(HttpServletRequest request,
			@PathVariable(value ="classId") String classId,
			@PathVariable(value="courseId") String courseId,@PathVariable(value="unitId") String unitId,@PathVariable(value="lessonId") String lessonId,@PathVariable(value="collectionId") String collectionId,@PathVariable(value="collectionType") String collectionType,@RequestParam(value = "userId", required = false) String userId, HttpServletResponse response) throws IOException{
		File file = classExportService.exportCsv(classId, courseId, unitId, lessonId, collectionId, ApiConstants.COLLECTION, userId);
		generateCSVOutput(response,file);
	}
}
