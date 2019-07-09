package javautils.json;

import net.sf.json.JSON;
import net.sf.json.xml.XMLSerializer;

public class JSONUtil {
	
	public static JSON toJSONString(String xml) {
		XMLSerializer xmlSerializer = new XMLSerializer();
		JSON json = xmlSerializer.read(xml);
		return json;
	}
	
}
