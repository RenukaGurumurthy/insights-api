package org.gooru.insights.api.controllers;

import java.io.IOException;
import java.text.ParseException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.services.ClassV2Service;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping(value="/v2/")
public class ClassV2Controller {

	@Autowired
	private ClassV2Service classService;

	private ClassV2Service getClassService() {
		return classService;
	}
	
	//TODO 	Test code to be removed
	@RequestMapping(value="/class/insert", method = {RequestMethod.GET})
	public void insertClass(HttpServletRequest request,
			HttpServletResponse response) throws JSONException, ParseException,
			IOException {

		getClassService().insertClassData();
	}
}
