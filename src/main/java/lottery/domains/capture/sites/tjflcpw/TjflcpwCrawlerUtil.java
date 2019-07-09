// package lottery.domains.capture.sites.tjflcpw;
//
// import javautils.StringUtil;
// import javautils.date.DateUtil;
// import javautils.http.EasyHttpClient;
// import lottery.domains.capture.utils.CodeValidate;
// import lottery.domains.content.entity.LotteryOpenCode;
// import net.sf.json.JSONArray;
// import net.sf.json.JSONObject;
// import org.apache.commons.httpclient.NameValuePair;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
//
// public class TjflcpwCrawlerUtil {
//
// 	private static final Logger logger = LoggerFactory.getLogger(TjflcpwCrawlerUtil.class);
// 	private static Map<String, EasyHttpClient> httpClientPool = new HashMap<>();
//
// 	private static EasyHttpClient getHttpClient(String lotteryName) {
// 		if (!httpClientPool.containsKey(lotteryName)) {
// 			httpClientPool.put(lotteryName, new EasyHttpClient());
// 		}
// 		return httpClientPool.get(lotteryName);
// 	}
//
// 	private TjflcpwCrawlerUtil() {
//
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getTjsscOpenCode(
// 			String lotteryName, String expect) {
// 		logger.info("获取天津时时彩开奖号码....");
// 		String url = "http://www.tjflcpw.com/Handlers/WinNumHandler.ashx";
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String _expect = expect.substring(2).replace("-", "");
// 		System.out.println(_expect);
// 		NameValuePair[] params = { new NameValuePair("termCode", _expect),
// 				new NameValuePair("playid", "4") };
// 		String data = httpClient.post(url, params);
// 		List<LotteryOpenCode> tmplist = getOpenCode(lotteryName, expect, data);
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		for (LotteryOpenCode tmpBean : tmplist) {
// 			if (CodeValidate.isSsc(tmpBean.getCode())) {
// 				list.add(tmpBean);
// 			}
// 		}
// 		return list;
// 	}
//
// 	public static void main(String[] args) {
// 		System.out.println(JSONArray.fromObject(getTjsscOpenCode("tjssc",
// 				"20150317-001")));
// 		System.out.println(JSONArray.fromObject(getTjsscOpenCode("tjssc",
// 				"20150317-001")));
// 	}
//
// 	private static List<LotteryOpenCode> getOpenCode(String lotteryName,
// 			String expect, String data) {
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		if (StringUtil.isNotNull(data)) {
// 			JSONObject json = JSONObject.fromObject(data);
// 			if (!json.get("Data").equals("null")) {
// 				String code = json.getJSONObject("Data").getString("WinNum");
// 				code = transCode(code);
// 				String time = DateUtil.getCurrentTime();
// 				LotteryOpenCode lotteryOpenCode = new LotteryOpenCode(
// 						lotteryName, expect, code, time, 0);
// 				list.add(lotteryOpenCode);
// 			}
// 		}
// 		return list;
// 	}
//
// 	private static String transCode(String code) {
// 		code = code.replaceAll("<li>|</li>", "");
// 		StringBuffer sb = new StringBuffer();
// 		for (int i = 0, j = code.length(); i < j; i++) {
// 			sb.append(code.substring(i, i + 1));
// 			if (i < j - 1) {
// 				sb.append(",");
// 			}
// 		}
// 		return sb.toString();
// 	}
//
// }