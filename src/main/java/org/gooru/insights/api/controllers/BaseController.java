/*******************************************************************************
 * BaseController.java
 * insights-read-api
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.gooru.insights.api.controllers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.poi.util.IOUtils;
import org.json.JSONObject;
import org.springframework.web.servlet.ModelAndView;

public class BaseController {
	
	List<Map<String,String>> errorData = new ArrayList<Map<String,String>>();
	
	protected ModelAndView getModel(List<Map<String,Object>> data,List<Map<String,String>> errorData){
		return  this.resultSet(data,errorData);
	}
	protected ModelAndView getModelString(List<Map<String,String>> data,List<Map<String,String>> errorData){
		return  this.resultSetString(data,errorData);
	}
	protected ModelAndView getModel(Map<String,Object> data,List<Map<String,String>> errorData){
		return  this.resultSet(data,errorData);
	}
	protected ModelAndView getModelObject(List<Map<Object,Object>> data,List<Map<String,String>> errorData){
		return  this.resultSetObject(data,errorData);
	}
	public ModelAndView resultSetString(List<Map<String,String>> data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){

			model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}
	protected ModelAndView getModel(JSONObject data,List<Map<String,String>> errorData){
		return  this.resultSet(data,errorData);
	}
	
	public ModelAndView resultSet(List<Map<String,Object>> data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){

			model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}
	
	public ModelAndView resultSet(Map<String,Object> data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){

			model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}
	
	public ModelAndView resultSet(JSONObject data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){
		model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}
	
	public void addFilterDate(ModelAndView model,List<Map<String,String>> errorData){
		if(errorData != null){
	
			List<Map<String,String>> dateRange = new ArrayList<Map<String,String>>();
			Map<String,String> filterDate = new HashMap<String,String>();
			String unixStartDate = null;
			String unixEndDate = null;
			String totalRows = null;
			for(Map<String,String> values : errorData){
				unixStartDate  = values.get("unixStartDate");
				unixEndDate = values.get("unixEndDate");
				totalRows = values.get("totalRows");
				if(unixStartDate != null){
					filterDate.put("unixStartDate", unixStartDate);
					values.remove("unixStartDate");
				}
				if(unixEndDate != null){
					filterDate.put("unixEndDate", unixEndDate);
					values.remove("unixEndDate");
				}
				if(totalRows != null){
					values.remove("totalRows");
				}
				
			}
			dateRange.add(filterDate);
			
			model.addObject("dateRange",dateRange);
			filterDate = new HashMap<String, String>();
			dateRange = new ArrayList<Map<String,String>>() ;
			if(totalRows != null){
				filterDate.put("totalRows",totalRows);
				dateRange.add(filterDate);
				
			}
			model.addObject("paginate",filterDate);
	}
	}
	public ModelAndView getModel(String emailId) {
		
		ModelAndView model = new ModelAndView("content");
		String data = null;
		if (emailId != null && emailId != "") {

			data = "Hi,This report will take some more time to get process,we will send you this report to " + emailId + " ,Thanks";

		} else {

			data = "Hi,This report will take some more time to get process,we will send you this report to insights@goorulearning.org, Thanks";

		}

		return model.addObject("content", data);

	}
	
	public List<Map<String,String>> handleErrors(){
	return this.errorData;
	}
	
	public void clearErrors(){
		this.errorData = new ArrayList<Map<String,String>>();
	}
	
	public String checkRequestContentType(HttpServletRequest request){
		
		String fileExtension = null;
		if(request.getHeader("Accept").contains("application/vnd.ms-excel"))
		{
			fileExtension = "xls";
		}
		else {
			fileExtension = "csv";
		}		
		return fileExtension;
	}
	

	public void generateExcelOutput(HttpServletResponse response, File excelFile) throws IOException {
		InputStream sheet = new FileInputStream(excelFile);
		response.setContentType("application/xls");
		IOUtils.copy(sheet, response.getOutputStream());
		response.getOutputStream().flush();
		response.getOutputStream().close();
	}
	
	public ModelAndView resultSetObject(List<Map<Object, Object>> data,List<Map<String,String>> errorData){
		ModelAndView model = new ModelAndView("content");
			model.addObject("content", data);
		addFilterDate(model, errorData);
		if(errorData != null){

			model.addObject("message", errorData);
		
		}
		clearErrors();
		return model;
	}

}
