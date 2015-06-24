package org.gooru.insights.api.controllers;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.ApiConstants.ModelAttributes;
import org.gooru.insights.api.models.ResponseParamDTO;
import org.springframework.web.servlet.ModelAndView;

import flexjson.JSONSerializer;

public class BaseController {

        public ModelAndView getModel(Object content) {

                ModelAndView model = new ModelAndView(ModelAttributes.CONTENT.getAttribute());
                if(content != null){
                        model.addObject(ModelAttributes.CONTENT.getAttribute(), new JSONSerializer().exclude(ApiConstants.EXCLUDE_CLASSES).deepSerialize(content));
                }else{
                        model.addObject(ModelAttributes.MESSAGE.getAttribute(), ApiConstants.DEFAULT_MAIL_MESSAGE);
                }
                return model;
        }

        public static String getSessionToken(HttpServletRequest request) {

                if (request.getHeader(ApiConstants.GOORU_SESSION_TOKEN) != null) {
                        return request.getHeader(ApiConstants.GOORU_SESSION_TOKEN);
                } else {
                        return request.getParameter(ApiConstants.SESSION_TOKEN);
                }
        }
        
        public ModelAndView getModel(Map<String, Object> content) {

            ModelAndView model = new ModelAndView(ModelAttributes.CONTENT.getAttribute());
            if(content != null && !content.isEmpty()){
                    model.addObject(ModelAttributes.CONTENT.getAttribute(), new JSONSerializer().exclude(ApiConstants.EXCLUDE_CLASSES).deepSerialize(content));
            }else{
                    model.addObject(ModelAttributes.MESSAGE.getAttribute(), ApiConstants.DEFAULT_MAIL_MESSAGE);
            }
            return model;
        }
        
    	public <M> ModelAndView getModel(ResponseParamDTO<M> data) {
    		ModelAndView model = new ModelAndView(ModelAttributes.VIEW_NAME.getAttribute());
    		model.addObject(ModelAttributes.RETURN_NAME.getAttribute() , new JSONSerializer().exclude(ApiConstants.EXCLUDE_CLASSES).deepSerialize(data));
    		
    		return model;
    	}
}