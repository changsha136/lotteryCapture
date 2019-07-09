// package lottery.domains.capture.sites.shishicai;
//
// import javautils.StringUtil;
// import javautils.date.DateUtil;
// import lottery.domains.capture.utils.CodeValidate;
// import lottery.domains.content.entity.LotteryOpenCode;
// import net.sf.json.JSONArray;
// import net.sf.json.JSONObject;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.util.*;
//
// public class ShiShiCaiCrawlerUtil {
//
// 	private static final Logger logger = LoggerFactory.getLogger(ShiShiCaiCrawlerUtil.class);
// 	private static Map<String, ShiShiCaiHttpClient> httpClientPool = new HashMap<>();
//
// 	private static ShiShiCaiHttpClient getHttpClient(String lotteryName) {
// 		if (!httpClientPool.containsKey(lotteryName)) {
// 			httpClientPool.put(lotteryName,
// 					new ShiShiCaiHttpClient(lotteryName));
// 		}
// 		return httpClientPool.get(lotteryName);
// 	}
//
// 	private ShiShiCaiCrawlerUtil() {
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getCqsscOpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取重庆时时彩开奖号码，日期为：" + date);
// 		String getUrl = "http://data.shishicai.cn/cqssc/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "4";
// 		String type = "ssc";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getJxsscOpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取江西时时彩开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/jxssc/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "5";
// 		String type = "ssc";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getSd11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取山东11选5开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/sd11x5/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "16";
// 		String type = "11x5";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getGd11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取广东11选5开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/gd11x5/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "24";
// 		String type = "11x5";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getJx11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取江西11选5开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/jx11x5/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "23";
// 		String type = "11x5";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getCq11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取重庆11选5开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/cq11x5/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "36";
// 		String type = "11x5";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getAh11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取安徽11选5开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/ah11x5/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "38";
// 		String type = "11x5";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getSh11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取上海11选5开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/sh11x5/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "37";
// 		String type = "11x5";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getJsk3OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取江苏快3开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/jsk3/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "3";
// 		String type = "k3";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getAhk3OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取安徽快3开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/ahk3/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "32";
// 		String type = "k3";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getHbk3OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取湖北快3开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/hbk3/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "31";
// 		String type = "k3";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getJlk3OpenCode(
// 			String lotteryName, String date) {
// 		logger.debug("获取吉林快3开奖号码....");
// 		String getUrl = "http://data.shishicai.cn/jlk3/haoma/";
// 		String postUrl = "http://data.shishicai.cn/handler/kuaikai/data.ashx";
// 		String lottery = "39";
// 		String type = "k3";
// 		ShiShiCaiHttpClient client = getHttpClient(lotteryName);
// 		String data = client.handleData(getUrl, postUrl, lottery, date);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static void main(String[] args) throws InterruptedException {
// 		List<LotteryOpenCode> list = getCq11x5OpenCode("cq11x5", "2015-03-18");
// 		for (LotteryOpenCode lotteryOpenCode : list) {
// 			System.out.println(JSONObject.fromObject(lotteryOpenCode));
// 		}
// 	}
//
// 	private static List<LotteryOpenCode> handleData(String lotteryName,
// 			String type, String data) {
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		if (StringUtil.isNotNull(data)) {
// 			try {
// 				Map<String, LotteryOpenCode> codeMap = new HashMap<>();
// 				JSONArray jsonList = JSONArray.fromObject(data);
// 				int length = jsonList.size();
// 				String decodeStr = (String) jsonList.get(length - 1);
// 				Map<String, String> decodeMap = getDecodeMap(decodeStr, type);
// 				for (int i = 0; i < length - 1; i++) {
// 					String[] s = String.valueOf(jsonList.get(i)).split(";");
// 					String expect = s[0];
// 					String code = decodeNumber(s[1], decodeMap, type);
// 					String time = DateUtil.getCurrentTime();
// 					LotteryOpenCode bean = new LotteryOpenCode(lotteryName,
// 							expect, code, time, 0);
// 					codeMap.put(expect, bean);
// 				}
// 				Object[] keys = codeMap.keySet().toArray();
// 				Arrays.sort(keys);
// 				for (Object o : keys) {
// 					LotteryOpenCode tmpBean = codeMap.get(o);
// 					switch (type) {
// 					case "ssc":
// 						if (CodeValidate.isSsc(tmpBean.getCode())) {
// 							list.add(tmpBean);
// 						}
// 						break;
// 					case "11x5":
// 						if (CodeValidate.is11x5(tmpBean.getCode())) {
// 							list.add(tmpBean);
// 						}
// 						break;
// 					case "k3":
// 						if (CodeValidate.isK3(tmpBean.getCode())) {
// 							list.add(tmpBean);
// 						}
// 						break;
// 					default:
// 						break;
// 					}
// 				}
// 			} catch (Exception e) {
// 				logger.error("解析数据失败...", e);
// 			}
// 		}
// 		return list;
// 	}
//
// 	private static Map<String, String> getDecodeMap(String s, String t) {
// 		Map<String, String> map = new HashMap<>();
// 		try {
// 			String[] a = s.substring(0, s.indexOf("_")).split(",");
// 			switch (t) {
// 			case "11x5":
// 				for (int i = 0; i < a.length; i++) {
// 					String key = String.format("%02d", Integer.parseInt(a[i]));
// 					String value = String.format("%02d", i + 1);
// 					map.put(key, value);
// 				}
// 				break;
// 			case "ssc":
// 				for (int i = 0; i < a.length; i++) {
// 					String key = String.valueOf(a[i]);
// 					String val = String.valueOf(i);
// 					map.put(key, val);
// 				}
// 				break;
// 			case "k3":
// 				for (int i = 0; i < a.length; i++) {
// 					String key = String.valueOf(a[i]);
// 					String val = String.valueOf(i + 1);
// 					map.put(key, val);
// 				}
// 				break;
// 			default:
// 				break;
// 			}
// 		} catch (Exception e) {
// 		}
// 		return map;
// 	}
//
// 	private static String decodeNumber(String s, Map<String, String> m, String t) {
// 		StringBuffer sb = new StringBuffer();
// 		switch (t) {
// 		case "11x5":
// 			String[] a = s.split(",");
// 			for (int i = 0, l = a.length; i < l; i++) {
// 				sb.append(m.get(a[i]));
// 				if (i != l - 1)
// 					sb.append(",");
// 			}
// 			break;
// 		case "ssc":
// 		case "k3":
// 			for (int i = 0, l = s.length(); i < l; i++) {
// 				sb.append(m.get(s.substring(i, i + 1)));
// 				if (i != l - 1)
// 					sb.append(",");
// 			}
// 			break;
// 		default:
// 			break;
// 		}
// 		return sb.toString();
// 	}
// }