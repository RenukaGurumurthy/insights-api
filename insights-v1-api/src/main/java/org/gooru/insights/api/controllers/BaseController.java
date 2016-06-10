package org.gooru.insights.api.controllers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServletResponse;

import org.apache.poi.util.IOUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.apiHeaders;
import org.gooru.insights.api.constants.ApiConstants.modelAttributes;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.servlet.ModelAndView;

import flexjson.JSONSerializer;
import rx.Observable;

public class BaseController {


	@Deprecated
	public <M> ModelAndView getModel(ResponseParamDTO<M> data) {

		ModelAndView model = new ModelAndView(modelAttributes.VIEW_NAME.getAttribute());
		model.addObject(modelAttributes.RETURN_NAME.getAttribute() , new JSONSerializer().exclude(ApiConstants.EXCLUDE_CLASSES).deepSerialize(data));
		return model;
	}

	public <M> DeferredResult<M> getDeferredResult(Observable<M> peersObserver) {
		DeferredResult<M> defferedResponse = new DeferredResult<>();
		peersObserver.subscribe(defferedResponse::setResult, defferedResponse::setErrorResult);
		return defferedResponse;
	}

	public HttpServletResponse setAllowOrigin(HttpServletResponse response) {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
		response.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE");
		return response;
	}

	public void generateCSVOutput(HttpServletResponse response, File csvFile) throws IOException {
		InputStream sheet = new FileInputStream(csvFile);
		response.setContentType(apiHeaders.CSV_RESPONSE.apiHeader());
		response.setHeader("Content-Disposition", "attachment; filename=\""+csvFile.getName()+ '"');
		IOUtils.copy(sheet, response.getOutputStream());
		response.getOutputStream().flush();
		csvFile.delete();
		response.getOutputStream().close();
	}
}
