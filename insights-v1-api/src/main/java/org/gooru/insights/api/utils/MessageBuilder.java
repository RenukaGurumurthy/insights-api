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

	private static final String DEFAULT_MESSAGE = "Please contact developer!";

	private static final String DEFAULT_MESSAGE_FILE_PATH = "message-properties/message";

	private static final String MESSAGE_FILE_PATH = "message.file.path";

	private static final String LOCALE_LANG = "locale.lang";

	private static final String LOCALE_COUNTRY = "locale.country";

	private static final String DEFAULT_LOCALE_LANG = "en";

	private static final String DEFAULT_LOCALE_COUNTRY = "US";

	private static ResourceBundle resourceBundle = null;

	@Resource(name = "constants")
	private Properties constants;

	@PostConstruct
	private void init() {

		Locale locale = new Locale((!constants.getProperty(LOCALE_LANG).startsWith("$") ?
			constants.getProperty(LOCALE_LANG) : DEFAULT_LOCALE_LANG),
				!constants.getProperty(LOCALE_COUNTRY).startsWith("$") ? constants.getProperty(LOCALE_COUNTRY) : DEFAULT_LOCALE_COUNTRY);

		resourceBundle = ResourceBundle.getBundle(!constants.getProperty(MESSAGE_FILE_PATH).startsWith("$") ?
			constants.getProperty(MESSAGE_FILE_PATH) : DEFAULT_MESSAGE_FILE_PATH, locale);
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
		StringBuilder stringBuffer = new StringBuilder();
		for (Object aText : text) {
			stringBuffer.append(aText);
		}
		return stringBuffer.toString();
	}
}
