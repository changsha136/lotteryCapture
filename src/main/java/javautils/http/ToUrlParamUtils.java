package javautils.http;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * url工具类
 * 
 * @author Mick
 * @since 2015-9-3
 */
public class ToUrlParamUtils {
	private static final String EMPTY = "";
	private static final String EQUALS = "=";
	private static final String DEFAULT_SEPARATOR = "&";
	
	public static String toUrlParam(Map<String, String> params) {
		return toUrlParam(params, DEFAULT_SEPARATOR, true);
	}
	
	public static String toUrlParam(Map<String, String> params, String separator) {
		return toUrlParam(params, separator, true);
	}
	
	public static String toUrlParam(Map<String, String> params, String separator, boolean ignoreEmpty) {
		if (params == null || params.isEmpty()) {
			return EMPTY;
		}
		
		StringBuffer url = new StringBuffer();
		
		Iterator<Entry<String, String>> it = params.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, String> entry = it.next();
			String value = entry.getValue();
			boolean valueIsEmpty = isEmpty(value);
			if (ignoreEmpty && valueIsEmpty) { // 主动忽略空值并且值为空
				continue;
			}
			
			url.append(entry.getKey()).append(EQUALS) .append(value);
			if (it.hasNext()) {
				url.append(separator);
			}
		}
		
		return url.toString();
	}
	
	private static boolean isEmpty(String value) {
		return value == null || EMPTY.equals(value);
	}
}
