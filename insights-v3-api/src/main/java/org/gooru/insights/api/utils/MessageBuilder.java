package org.gooru.insights.api.utils;

import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.gooru.insights.api.constants.ApiConstants;
import org.springframework.stereotype.Component;

@Component
public class MessageBuilder {

	private static final String DEFAULT_MESSAGE = "message not found!";

	private static final String DEFAULT_MESSAGE_FILE_PATH = "message-properties/message";
	
	private static final String MESSAGE_FILE_PATH = "message.file.path";

	private static final String LOCALE_LANG = "locale.lang";

	private static final String LOCALE_COUNTRY = "locale.country";

	private static final String DEFAULT_LOCALE_LANG = "en";

	private static final String DEFAULT_LOCALE_COUNTRY = "US";

	private static ResourceBundle resourceBundle = null;

	@Resource(name = "gooruConstants")
	private Properties gooruConstants;

	@PostConstruct
	private void init() {

		Locale locale = new Locale((!gooruConstants.getProperty(LOCALE_LANG).startsWith("$") ? gooruConstants.get(LOCALE_LANG).toString() : DEFAULT_LOCALE_LANG),
				!gooruConstants.getProperty(LOCALE_COUNTRY).startsWith("$") ? gooruConstants.get(LOCALE_COUNTRY).toString() : DEFAULT_LOCALE_COUNTRY);
		
		resourceBundle = ResourceBundle.getBundle(!gooruConstants.getProperty(MESSAGE_FILE_PATH).startsWith("$") ? gooruConstants.get(MESSAGE_FILE_PATH).toString() : DEFAULT_MESSAGE_FILE_PATH, locale);
	}

	/**
	 * This will provide the value in the localizer
	 * 
	 * @param key
	 *            will be the fetch key
	 * @return value returned as string
	 */
	public static String getMessage(String key) {

		if (resourceBundle.containsKey(key)) {
			return resourceBundle.getString(key);
		}
		return DEFAULT_MESSAGE;
	}

	/**
	 * 
	 * @param key
	 * @param replacer
	 * @return
	 */
	public static String getMessage(String key, String... replacer) {

		if (resourceBundle.containsKey(key)) {
			String value = resourceBundle.getString(key);
			for (int i = 0; i < replacer.length; i++) {
				value = value.replace(buildString(new Object[] { ApiConstants.OPEN_BRACE, i, ApiConstants.CLOSE_BRACE }), replacer[i]);
			}
			return value;
		}
		return DEFAULT_MESSAGE;
	}

	public static String buildString(Object[] text) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < text.length; i++) {
			stringBuffer.append(text[i]);
		}
		return stringBuffer.toString();
	}
}
