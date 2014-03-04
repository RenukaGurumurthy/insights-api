/*******************************************************************************
 * RestErrorConverter.java
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
package org.gooru.insights.api.web.servlet.handlers;

import org.springframework.core.convert.converter.Converter;

/**
 * A {@code RestErrorConverter} is an intermediate 'bridge' component in the response rendering pipeline: it converts
 * a {@link RestError} object into another object that is potentially better suited for HTTP response rendering by an
 * {@link org.springframework.http.converter.HttpMessageConverter HttpMessageConverter}.
 * <p/>
 * For example, a {@code RestErrorConverter} implementation might produce an intermediate Map of name/value pairs.
 * This resulting map might then be given to an {@code HttpMessageConverter} to write the response body:
 * <pre>
 *     Object result = mapRestErrorConverter.convert(aRestError);
 *     assert result instanceof Map;
 *     ...
 *     httpMessageConverter.write(result, ...);
 * </pre>
 * <p/>
 * This allows spring configurers to use or write simpler RestError conversion logic and let the more complex registered
 * {@code HttpMessageConverter}s operate on the converted result instead of needing to implement the more
 * complex {@code HttpMessageConverter} interface directly.
 *
 * @param <T> The type of object produced by the converter.
 *
 * @see org.springframework.http.converter.HttpMessageConverter
 * @see Converter
 *
 * @author Les Hazlewood
 */
public interface RestErrorConverter<T> extends Converter<RestError, T> {

    /**
     * Converts the RestError instance into an object that will then be used by an
     * {@link org.springframework.http.converter.HttpMessageConverter HttpMessageConverter} to render the response body.
     *
     * @param re the {@code RestError} instance to convert to another object instance 'understood' by other registered
     *           {@code HttpMessageConverter} instances.
     * @return an object suited for HTTP response rendering by an {@code HttpMessageConverter}
     */
    T convert(RestError re);
}
