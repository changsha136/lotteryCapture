// package lottery.domains.capture.sites.hn481;
//
// import cryptix.jce.provider.MD5;
// import javautils.date.Moment;
// import javautils.http.EasyHttpClient;
// import javautils.json.JSONUtil;
// import lottery.domains.capture.utils.CodeValidate;
// import lottery.domains.content.entity.LotteryOpenCode;
// import net.sf.json.JSONArray;
// import net.sf.json.JSONObject;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import sun.misc.BASE64Decoder;
// import sun.misc.BASE64Encoder;
//
// import java.util.*;
//
// public class Hn481CrawlerUtil {
//
// 	private static final Logger logger = LoggerFactory.getLogger(Hn481CrawlerUtil.class);
// 	private static Map<String, EasyHttpClient> httpClientPool = new HashMap<>();
//
// 	private static EasyHttpClient getHttpClient(String lotteryName) {
// 		if (!httpClientPool.containsKey(lotteryName)) {
// 			httpClientPool.put(lotteryName, new EasyHttpClient());
// 		}
// 		return httpClientPool.get(lotteryName);
// 	}
//
// 	private Hn481CrawlerUtil() {
//
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getOpenCode(
// 			String lotteryName) {
// 		logger.info("获取河南481开奖号码....");
// 		String url = "http://m.hn481.com:8098/TradeSystem/SearchLotteryResultListService.aspx";
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String result = httpClient.postString(url, getRequestString());
// 		List<LotteryOpenCode> tmplist = decode(lotteryName, result);
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		for (LotteryOpenCode tmpBean : tmplist) {
// 			if (CodeValidate.isKy481(tmpBean.getCode())) {
// 				list.add(tmpBean);
// 			}
// 		}
// 		return list;
// 	}
//
// 	public static List<LotteryOpenCode> decode(String lotteryName, String data) {
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		try {
// 			byte[] decodeBuffer = new BASE64Decoder().decodeBuffer(data);
// 			String result = new String(decodeBuffer);
// 			result = result.trim().replaceFirst("^([\\W]+)<", "<");
// 			JSONObject json = JSONObject.fromObject(JSONUtil
// 					.toJSONString(result));
// 			JSONObject body = (JSONObject) json.get("body");
// 			JSONArray elements = body.getJSONArray("elements");
// 			Map<String, LotteryOpenCode> tmpMap = new HashMap<>();
// 			String time = new Moment().toSimpleTime();
// 			for (Object o : elements) {
// 				JSONObject r = (JSONObject) o;
// 				String expect = formatExpect(r.getString("issue"));
// 				String code = formatCode(r.getString("result"));
// 				LotteryOpenCode bean = new LotteryOpenCode(lotteryName, expect,
// 						code, time, 0);
// 				tmpMap.put(expect, bean);
// 			}
// 			Object[] keys = tmpMap.keySet().toArray();
// 			Arrays.sort(keys);
// 			for (Object o : keys) {
// 				list.add(tmpMap.get(o));
// 			}
// 		} catch (Exception e) {
// 			logger.error("解析河南481号码失败！", e);
// 		}
// 		return list;
// 	}
//
// 	public static void main(String[] args) {
// 		List<LotteryOpenCode> list = getOpenCode("hn481");
// 		for (LotteryOpenCode lotteryOpenCode : list) {
// 			System.out.println(JSONObject.fromObject(lotteryOpenCode));
// 		}
// 	}
//
// 	public static String getRequestString() {
// 		int lotteryId = 81, merchantid = 88;
// 		StringBuilder bodyBuilder = new StringBuilder();
// 		bodyBuilder.append("<body>");
// 		bodyBuilder.append("<lotteryid>" + lotteryId + "</lotteryid>");
// 		bodyBuilder.append("</body>");
// 		StringBuffer xmlBuffer = new StringBuffer();
// 		String messengerid = new Moment().format("yyyyMMddHHmmss");
// 		String digest = new MD5()
// 				.toMD5(messengerid + "000001" + messengerid + merchantid
// 						+ bodyBuilder).toLowerCase();
// 		xmlBuffer.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
// 		xmlBuffer.append("<message>");
// 		xmlBuffer.append("<header>");
// 		xmlBuffer
// 				.append("<messengerid>" + messengerid + "000001</messengerid>");
// 		xmlBuffer.append("<timestamp>" + messengerid + "</timestamp>");
// 		xmlBuffer.append("<digest>" + digest + "</digest>");
// 		xmlBuffer.append("<merchantid>" + merchantid + "</merchantid>");
// 		xmlBuffer.append("</header>");
// 		xmlBuffer.append(bodyBuilder);
// 		xmlBuffer.append("</message>");
// 		return new BASE64Encoder().encode(xmlBuffer.toString().getBytes());
// 	}
//
// 	public static String formatExpect(String expect) {
// 		return "20" + expect.substring(0, 6) + "-"
// 				+ String.format("%03d", Integer.parseInt(expect.substring(6)));
// 	}
//
// 	public static String formatCode(String code) {
// 		StringBuffer sb = new StringBuffer();
// 		for (int i = 0, j = code.length(); i < j; i++) {
// 			sb.append(code.substring(i, i + 1));
// 			if (i != j - 1)
// 				sb.append(",");
// 		}
// 		return sb.toString();
// 	}
//
// }