package org.gooru.insights.api.controllers;

import javax.servlet.http.HttpServletRequest;

import org.gooru.insights.api.spring.exception.UnKnownResourceException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;


/**
 * Default controller that exists to return a proper REST response for unmapped requests.
 */
@Controller
public class DefaultController {

    @RequestMapping(value="/**")
    public void unmappedRequest(HttpServletRequest request) {
        String uri = request.getRequestURI();
        throw new UnKnownResourceException("There is no resource for path " + uri);
    }
}
