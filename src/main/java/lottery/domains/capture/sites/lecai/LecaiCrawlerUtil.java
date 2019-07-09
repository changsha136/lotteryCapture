// package lottery.domains.capture.sites.lecai;
//
// import javautils.StringUtil;
// import javautils.date.DateUtil;
// import javautils.html.HtmlUtils;
// import javautils.http.EasyHttpClient;
// import lottery.domains.capture.utils.CodeValidate;
// import lottery.domains.content.entity.LotteryOpenCode;
// import net.sf.json.JSONObject;
// import org.htmlparser.Node;
// import org.htmlparser.util.NodeList;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.util.*;
//
// public class LecaiCrawlerUtil {
//
// 	private static final Logger logger = LoggerFactory.getLogger(LecaiCrawlerUtil.class);
// 	private static Map<String, EasyHttpClient> httpClientPool = new HashMap<>();
//
// 	private static EasyHttpClient getHttpClient(String lotteryName) {
// 		if (!httpClientPool.containsKey(lotteryName)) {
// 			httpClientPool.put(lotteryName, new EasyHttpClient());
// 		}
// 		return httpClientPool.get(lotteryName);
// 	}
//
// 	private LecaiCrawlerUtil() {
//
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getPl3OpenCode(String lotteryName) {
// 		logger.info("获取排列3开奖号码....");
// 		String url = "http://baidu.lecai.com/lottery/draw/list/3";
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		List<LotteryOpenCode> tmplist = getFc3dAndPl3(lotteryName, data);
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		for (LotteryOpenCode tmpBean : tmplist) {
// 			if(CodeValidate.is3d(tmpBean.getCode())) {
// 				list.add(tmpBean);
// 			}
// 		}
// 		return list;
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getBjkl8OpenCode(String lotteryName, String date) {
// 		logger.info("获取北京快乐8开奖号码....");
// 		String url = "http://baidu.lecai.com/lottery/draw/list/543?d=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		List<LotteryOpenCode> tmplist = getFc3dAndPl3(lotteryName, data);
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		for (LotteryOpenCode tmpBean : tmplist) {
// 			if(CodeValidate.isBjkl8(tmpBean.getCode())) {
// 				list.add(tmpBean);
// 			}
// 		}
// 		return list;
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getBjpk10OpenCode(String lotteryName, String date) {
// 		logger.info("获取北京PK拾开奖号码....");
// 		String url = "http://baidu.lecai.com/lottery/draw/list/557?d=" + date;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		List<LotteryOpenCode> tmplist = getFc3dAndPl3(lotteryName, data);
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		for (LotteryOpenCode tmpBean : tmplist) {
// 			if(CodeValidate.isBjpk10(tmpBean.getCode())) {
// 				list.add(tmpBean);
// 			}
// 		}
// 		return list;
// 	}
//
// 	public static void main(String[] args) {
// 		//List<LotteryOpenCode> lists = getBjkl8OpenCode("bjkl8", "2015-04-06");
// 		//List<LotteryOpenCode> lists = getBjpk10OpenCode("bjpk10", "2015-04-06");
// 		List<LotteryOpenCode> lists = getPl3OpenCode("pl3");
// 		for (LotteryOpenCode lotteryOpenCode : lists) {
// 			System.out.println(JSONObject.fromObject(lotteryOpenCode));
// 		}
// 	}
//
// 	private static List<LotteryOpenCode> getFc3dAndPl3(String lotteryName, String data) {
// 		int expectIndex = 1, ballsIndex = 2, codeLength = 1;
// 		if("bjkl8".equals(lotteryName)) {
// 			ballsIndex = 3;
// 			codeLength = 2;
// 		}
// 		if("bjpk10".equals(lotteryName)) {
// 			codeLength = 2;
// 		}
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		Map<String, LotteryOpenCode> map = new HashMap<>();
// 		if(StringUtil.isNotNull(data)) {
// 			try {
// 				String resource = data.replaceAll("(\t|\r|\n)", "");
// 				NodeList nList = HtmlUtils.FilterTag(resource, "table", "UTF-8");
// 				String currTime = DateUtil.getCurrentTime();
// 				if(nList.size() > 0) {
// 					NodeList trList = HtmlUtils.FilterTag(nList.elementAt(0).toString(), "tr", "UTF-8");
// 					Node[] nodes = trList.toNodeArray();
// 					for (int i = 1; i < nodes.length; i++) {
// 						LotteryOpenCode bean = new LotteryOpenCode();
// 						bean.setLottery(lotteryName);
// 						bean.setTime(currTime);
//
// 						NodeList tds = HtmlUtils.FilterTag(nodes[i].toHtml(), "td", "UTF-8");
// 						String expect = tds.elementAt(expectIndex).toPlainTextString().replaceAll(" ", "");
// 						NodeList balls = HtmlUtils.FilterCss(tds.elementAt(ballsIndex).toHtml(), ".ball_1", "UTF-8");
//
// 						String code = transCode(balls.asString(), codeLength);
// 						bean.setExpect(expect);
// 						bean.setCode(code);
// 						map.put(expect, bean);
// 					}
// 				}
// 				Object[] keys = map.keySet().toArray();
// 				Arrays.sort(keys);
// 				for (Object o : keys) {
// 					list.add(map.get(o));
// 				}
// 			} catch (Exception e) {
// 				logger.error("解析数据失败...", e);
// 			}
// 		}
// 		return list;
// 	}
//
// 	private static String transCode(String code, int cLen) {
// 		StringBuffer sb = new StringBuffer();
// 		for (int i = 0, j = code.length()/cLen; i < j; i++) {
// 			sb.append(code.substring(i * cLen, (i + 1) * cLen));
// 			if(i < j - 1) {
// 				sb.append(",");
// 			}
// 		}
// 		return sb.toString();
// 	}
//
// }