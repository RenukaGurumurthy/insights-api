/*******************************************************************************
 * Serializer.java
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
package org.gooru.insights.api.utils;

import org.springframework.web.servlet.ModelAndView;

import com.thoughtworks.xstream.XStream;

import flexjson.JSONSerializer;

public class Serializer {

	public static ModelAndView toModelAndView(Object object) {
		ModelAndView jsonmodel = new ModelAndView("model");
		jsonmodel.addObject("model", object);
		return jsonmodel;
	}

	public static ModelAndView toModelAndView(Object obj, String type, String[] excludes, String... includes) {
		return toModelAndView(serialize(obj, type, excludes, includes));
	}

	// public static String serialize(Object model, String type, String[]
	// excludes, boolean deepSerialize, String... includes) {
	// return toModelAndView(serialize(model, type, excludes, deepSerialize,
	// true, includes));
	// }
	public static ModelAndView toModelAndView(Object obj, String type, String[] excludes) {
		return toModelAndView(serialize(obj, type, excludes));
	}

	public static ModelAndView toModelAndView(Object obj, String type, boolean deepSerialize) {
		return toModelAndView(serialize(obj, type, deepSerialize, null, null));
	}

	public static ModelAndView toModelAndView(Object obj, String type) {
		return toModelAndView(serialize(obj, type, false, null, null));
	}

	public static String serialize(Object model, String type, boolean deepSerialize, String[] excludes, String... includes) {
		if (model == null) {
			return "";
		}
		String serializedData = null;
		JSONSerializer serializer = new JSONSerializer();

		if (type == null || type.equals("json")) {
			if (includes != null) {
				serializer.include(includes);
			}

			if (excludes != null) {
				serializer.exclude(excludes);
			}
		
			try {

				serializedData = deepSerialize ? serializer.deepSerialize(model) : serializer.serialize(model);

			} catch (Exception ex) {

				throw new RuntimeException(ex);
			}

		} else {
			serializedData = new XStream().toXML(model);
		}
		return serializedData;
	}

	public static String serialize(Object model, String type, String[] excludes, String... includes) {
		return serialize(model, type, false, excludes, includes);
	}

}
