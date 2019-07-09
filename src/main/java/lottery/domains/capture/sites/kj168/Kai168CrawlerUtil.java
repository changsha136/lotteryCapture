// package lottery.domains.capture.sites.kj168;
//
// import javautils.StringUtil;
// import javautils.date.DateUtil;
// import javautils.http.EasyHttpClient;
// import lottery.domains.capture.utils.CodeValidate;
// import lottery.domains.content.entity.LotteryOpenCode;
// import net.sf.json.JSONArray;
// import net.sf.json.JSONObject;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.util.*;
//
// public class Kai168CrawlerUtil {
//
// 	/**
// 	 * 重庆时时彩 http://www.168kai.com/History/HisList?id=10011&date=2014-10-20
// 	 * 江西时时彩 1002 新疆时时彩 10022 ------不需要 天津时时彩 10021 菲博分分彩 广东11选5 1007 江西11选5
// 	 * 1001 重庆11选5 10012 安徽11选5 10025 上海11选5 10024 十一运夺金 1003 江苏快3 1006 安徽快3
// 	 * 10043 湖北快3 吉林快3 10013 福彩3D 2002 排列三 2007 北京快乐8 10014 北京PK拾 10016
// 	 */
//
// 	private static final Logger logger = LoggerFactory.getLogger(Kai168CrawlerUtil.class);
// 	private static Map<String, EasyHttpClient> httpClientPool = new HashMap<>();
//
// 	private static EasyHttpClient getHttpClient(String lotteryName) {
// 		if (!httpClientPool.containsKey(lotteryName)) {
// 			httpClientPool.put(lotteryName, new EasyHttpClient());
// 		}
// 		return httpClientPool.get(lotteryName);
// 	}
//
// 	private Kai168CrawlerUtil() {
//
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getXjsscOpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取新疆时时彩开奖号码....");
// 		String code = "10022";
// 		String type = "ssc";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getCqsscOpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取重庆时时彩开奖号码....");
// 		String code = "10011";
// 		String type = "ssc";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getJxsscOpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取江西时时彩开奖号码....");
// 		String code = "1002";
// 		String type = "ssc";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getTjsscOpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取天津时时彩开奖号码....");
// 		String code = "10021";
// 		String type = "ssc";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getGd11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取广东11选5开奖号码....");
// 		String code = "1007";
// 		String type = "11x5";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getJx11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取江西11选5开奖号码....");
// 		String code = "1001";
// 		String type = "11x5";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getCq11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取重庆11选5开奖号码....");
// 		String code = "10012";
// 		String type = "11x5";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getAh11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取安徽11选5开奖号码....");
// 		String code = "10025";
// 		String type = "11x5";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getSh11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取上海11选5开奖号码....");
// 		String code = "10024";
// 		String type = "11x5";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getSd11x5OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取山东11选5开奖号码....");
// 		String code = "1003";
// 		String type = "11x5";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getJsk3OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取江苏快3开奖号码....");
// 		String code = "1006";
// 		String type = "k3";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getAhk3OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取安徽快3开奖号码....");
// 		String code = "10043";
// 		String type = "k3";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getJlk3OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取吉林快3开奖号码....");
// 		String code = "10013";
// 		String type = "k3";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getFc3dOpenCode(
// 			String lotteryName) {
// 		logger.info("获取福彩3d开奖号码....");
// 		String code = "2002";
// 		String type = "3d";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getPl3OpenCode(
// 			String lotteryName) {
// 		logger.info("获取排列3开奖号码....");
// 		String code = "2007";
// 		String type = "3d";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getBjkl8OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取北京快乐8开奖号码....");
// 		String code = "10014";
// 		String type = "bjkl8";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getBjpk10OpenCode(
// 			String lotteryName, String date) {
// 		logger.info("获取北京pk拾开奖号码....");
// 		String code = "10016";
// 		String type = "bjpk10";
// 		String url = "http://www.168kai.com/History/HisList?id=" + code
// 				+ "&date=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		return handleData(lotteryName, type, data);
// 	}
//
// 	private static List<LotteryOpenCode> handleData(String lotteryName,
// 			String type, String data) {
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		if (StringUtil.isNotNull(data)) {
// 			Map<String, LotteryOpenCode> tmpMap = new HashMap<String, LotteryOpenCode>();
// 			try {
// 				Object object = JSONObject.fromObject(data).get("list");
// 				JSONArray jsonArr = JSONArray.fromObject(object);
// 				String time = DateUtil.getCurrentTime();
// 				for (Object o : jsonArr) {
// 					JSONObject jsonObj = (JSONObject) o;
// 					String expect = jsonObj.getString("c_t");
// 					String code = jsonObj.getString("c_r");
// 					switch (type) {
// 					case "ssc":
// 						expect = expect.substring(0, 8)
// 								+ "-"
// 								+ String.format("%03d",
// 										Integer.parseInt(expect.substring(8)));
// 						break;
// 					case "11x5":
// 						if ("jx11x5".equals(lotteryName)
// 								|| "sh11x5".equals(lotteryName)) {
// 							try {
// 								expect = expect.substring(0, 8)
// 										+ "-"
// 										+ String.format("%03d", Integer
// 												.parseInt(expect.substring(8)));
// 							} catch (Exception e) {
// 								e.printStackTrace();
// 							}
//
// 						} else {
// 							expect = DateUtil.formatTime(
// 									expect.substring(0, 6), "yyMMdd",
// 									"yyyyMMdd")
// 									+ "-"
// 									+ String.format("%03d", Integer
// 											.parseInt(expect.substring(6)));
// 						}
// 						try {
// 							String[] codeArr = code.split(",");
// 							String codeFormat = "";
// 							for (String tmpCode : codeArr) {
// 								codeFormat += String.format("%02d",
// 										Integer.parseInt(tmpCode))
// 										+ ",";
// 							}
// 							code = codeFormat.substring(0,
// 									codeFormat.length() - 1);
// 						} catch (Exception e) {
// 						}
// 						break;
// 					case "k3":
// 						expect = DateUtil.formatTime(expect.substring(0, 6),
// 								"yyMMdd", "yyyyMMdd")
// 								+ "-"
// 								+ expect.substring(6);
// 						break;
// 					case "3d":
// 						if ("fc3d".equals(lotteryName)) {
// 							expect = expect.substring(2);
// 						}
// 						break;
// 					case "bjpk10":
// 						try {
// 							String[] codeArr = code.split(",");
// 							String codeFormat = "";
// 							for (String tmpCode : codeArr) {
// 								codeFormat += String.format("%02d",
// 										Integer.parseInt(tmpCode))
// 										+ ",";
// 							}
// 							code = codeFormat.substring(0,
// 									codeFormat.length() - 1);
// 						} catch (Exception e) {
// 						}
// 						break;
// 					case "bjkl8":
// 						try {
// 							String[] codeArr = code.split(",");
// 							String codeFormat = "";
// 							for (int i = 0; i < 20; i++) {
// 								codeFormat += String.format("%02d",
// 										Integer.parseInt(codeArr[i]))
// 										+ ",";
// 							}
// 							code = codeFormat.substring(0,
// 									codeFormat.length() - 1);
// 						} catch (Exception e) {
// 						}
// 						break;
// 					default:
// 						break;
// 					}
// 					LotteryOpenCode bean = new LotteryOpenCode(lotteryName,
// 							expect, code, time, 0);
// 					tmpMap.put(expect, bean);
// 				}
// 			} catch (Exception e) {
// 				logger.error("解析数据失败...", e);
// 			}
// 			Object[] keys = tmpMap.keySet().toArray();
// 			Arrays.sort(keys);
// 			for (Object o : keys) {
// 				LotteryOpenCode tmpBean = tmpMap.get(o);
// 				switch (type) {
// 				case "ssc":
// 					if (CodeValidate.isSsc(tmpBean.getCode())) {
// 						list.add(tmpBean);
// 					}
// 					break;
// 				case "11x5":
// 					if (CodeValidate.is11x5(tmpBean.getCode())) {
// 						list.add(tmpBean);
// 					}
// 					break;
// 				case "k3":
// 					if (CodeValidate.isK3(tmpBean.getCode())) {
// 						list.add(tmpBean);
// 					}
// 					break;
// 				case "3d":
// 					if (CodeValidate.is3d(tmpBean.getCode())) {
// 						list.add(tmpBean);
// 					}
// 					break;
// 				case "bjkl8":
// 					if (CodeValidate.isBjkl8(tmpBean.getCode())) {
// 						list.add(tmpBean);
// 					}
// 					break;
// 				case "bjpk10":
// 					if (CodeValidate.isBjpk10(tmpBean.getCode())) {
// 						list.add(tmpBean);
// 					}
// 					break;
// 				default:
// 					break;
// 				}
// 			}
// 		}
// 		return list;
// 	}
//
// 	public static void main(String[] args) {
// 		// List<LotteryOpenCode> list = getCqsscOpenCode("cqssc", "2014-12-20");
// 		// List<LotteryOpenCode> list = getJxsscOpenCode("jxssc", "2014-12-25");
// 		// List<LotteryOpenCode> list = getTjsscOpenCode("tjssc", "2014-12-25");
//
// 		// List<LotteryOpenCode> list = getGd11x5OpenCode("gd11x5",
// 		// "2014-12-25");
// 		// List<LotteryOpenCode> list = getJx11x5OpenCode("jx11x5",
// 		// "2014-12-25");
// 		// List<LotteryOpenCode> list = getCq11x5OpenCode("cq11x5",
// 		// "2014-12-25");
// 		// List<LotteryOpenCode> list = getAh11x5OpenCode("ah11x5",
// 		// "2014-12-25");
//
// 		// List<LotteryOpenCode> list = getSh11x5OpenCode("sh11x5",
// 		// "2014-12-25");
// 		// List<LotteryOpenCode> list = getSd11x5OpenCode("sd11x5",
// 		// "2014-12-25");
// 		// List<LotteryOpenCode> list = getJsk3OpenCode("jsk3", "2014-12-25");
// 		// List<LotteryOpenCode> list = getAhk3OpenCode("ahk3", "2014-12-25");
// 		// List<LotteryOpenCode> list = getJlk3OpenCode("jsk3", "2014-12-25");
//
// 		// List<LotteryOpenCode> list = getFc3dOpenCode("fc3d");
// 		// List<LotteryOpenCode> list = getPl3OpenCode("pl3");
//
// 		List<LotteryOpenCode> list = getBjkl8OpenCode("bjkl8", "2014-12-25");
// 		// List<LotteryOpenCode> list = getBjpk10OpenCode("bjpk10",
// 		// "2014-12-25");
// 		for (LotteryOpenCode lotteryOpenCode : list) {
// 			System.out.println(JSONObject.fromObject(lotteryOpenCode));
// 		}
// 	}
//
// }