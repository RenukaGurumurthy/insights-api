/*******************************************************************************
 * CollectionRestController.java
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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gooru.insights.api.models.InsightsConstant;
import org.gooru.insights.api.services.CassandraService;
import org.gooru.insights.api.services.CollectionRestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.scheduling.annotation.EnableAsync;

@Controller
@RequestMapping(value = "/collections", headers = "Accept=application/xml, application/json,application/xls ", produces = {
		"application/json", "application/xml", "application/xls" })
@EnableAsync
public class CollectionRestController extends BaseController implements
		InsightsConstant {

	@Autowired
	private CollectionRestService collectionService;

	@Autowired
	private CassandraService cassandraService;

	public CassandraService getCassandraService() {
		return cassandraService;
	}

	@RequestMapping(value = "", method = { RequestMethod.GET,
			RequestMethod.POST })
	@ResponseBody
	public ModelAndView getCollectionList(HttpServletRequest request,
			@RequestParam(value = DATA_OBJECT, required = true) String data,
			HttpServletResponse response) throws Exception {

		String databaseType = "mysql";
		return getModel(
				getCollectionServiceCall().getCollectionData(databaseType,
						data, null, handleErrors()), handleErrors());
	}

	@RequestMapping(value = "/{collectionId}", method = { RequestMethod.GET,
			RequestMethod.POST })
	@ResponseBody
	public ModelAndView getCollectionList(HttpServletRequest request,
			@RequestParam(value = DATA_OBJECT, required = true) String data,
			@PathVariable(value = "collectionId") String collectionId,
			HttpServletResponse response) throws Exception {

		String databaseType = "mysql";
		return getModel(
				getCollectionServiceCall().getCollectionData(databaseType,
						data, collectionId, handleErrors()), handleErrors());
	}

	@RequestMapping(value = "/resources", method = { RequestMethod.GET,
			RequestMethod.POST })
	@ResponseBody
	public ModelAndView getCollectionResourceDetail(HttpServletRequest request,
			@RequestParam(value = DATA_OBJECT, required = true) String data,
			HttpServletResponse response) throws Exception {

		return getModel(
				getCollectionServiceCall().getCollectionResourceDetail(data,
						null, null, handleErrors()), handleErrors());
	}

	@RequestMapping(value = "/{collectionId}/{resourceId}", method = {
			RequestMethod.GET, RequestMethod.POST })
	@ResponseBody
	public ModelAndView getCollectionResourceDetail(HttpServletRequest request,
			@RequestParam(value = DATA_OBJECT, required = true) String data,
			@PathVariable(value = "collectionId") String collectionId,
			@PathVariable(value = "resourceId") String resourceId,
			HttpServletResponse response) throws Exception {

		return getModel(
				getCollectionServiceCall().getCollectionResourceDetail(data,
						collectionId, resourceId, handleErrors()),
				handleErrors());
	}

	@RequestMapping(value = "/{collectionId}/resources", method = {
			RequestMethod.GET, RequestMethod.POST })
	@ResponseBody
	public ModelAndView getCollectionResourceDetail(HttpServletRequest request,
			@RequestParam(value = DATA_OBJECT, required = true) String data,
			@PathVariable(value = "collectionId") String collectionId,
			HttpServletResponse response) throws Exception {

		return getModel(
				getCollectionServiceCall().getCollectionResourceDetail(data,
						collectionId, null, handleErrors()), handleErrors());
	}

	private CollectionRestService getCollectionServiceCall() {

		return collectionService;

	}

	@RequestMapping(value = "{collectionId}/detail", method = {
			RequestMethod.GET, RequestMethod.POST })
	@ResponseBody
	public ModelAndView getEventDetail(HttpServletRequest request,
			@PathVariable(value = "collectionId") String collectionId,
			@RequestParam(value = DATA_OBJECT, required = false) String data,
			HttpServletResponse response) throws ParseException {
		String databaseType = "cassandra";
		return getModel(
				getCollectionServiceCall().getCollectionData(databaseType,
						data, collectionId, handleErrors()), handleErrors());

	}

	@RequestMapping(value = "resource/deleted", method = { RequestMethod.GET,
			RequestMethod.POST })
	@ResponseBody
	public ModelAndView markDeletedResource(HttpServletRequest request,
			@RequestParam(value = "startTime") String startTime,
			@RequestParam(value = "endTime") String endTime,
			HttpServletResponse response) throws ParseException {

		getCollectionServiceCall().markDeletedResource(startTime, endTime);

		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("status", "check with keyspace");
		return getModel(dataMap, new ArrayList<Map<String, String>>());

	}

}
