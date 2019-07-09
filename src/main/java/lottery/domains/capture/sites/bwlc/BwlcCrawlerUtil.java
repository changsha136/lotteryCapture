// package lottery.domains.capture.sites.bwlc;
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
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
//
// public class BwlcCrawlerUtil {
//
// 	private static final Logger logger = LoggerFactory.getLogger(BwlcCrawlerUtil.class);
// 	private static Map<String, EasyHttpClient> httpClientPool = new HashMap<>();
//
// 	private static EasyHttpClient getHttpClient(String lotteryName) {
// 		if (!httpClientPool.containsKey(lotteryName)) {
// 			httpClientPool.put(lotteryName, new EasyHttpClient());
// 		}
// 		return httpClientPool.get(lotteryName);
// 	}
//
// 	private BwlcCrawlerUtil() {
//
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getBjkl8OpenCode(String lotteryName, String expect) {
// 		logger.info("获取北京快乐8开奖号码....");
// 		String url = "http://www.bwlc.net/bulletin/prevkeno.html?num=" + expect;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		List<LotteryOpenCode> tmplist = getBjkl8AndBjpk10(lotteryName, data);
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		for (LotteryOpenCode tmpBean : tmplist) {
// 			if(CodeValidate.isBjkl8(tmpBean.getCode())) {
// 				list.add(tmpBean);
// 			}
// 		}
// 		return list;
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getBjpk10OpenCode(String lotteryName, String expect) {
// 		logger.info("获取北京PK拾开奖号码....");
// 		String url = "http://www.bwlc.net/bulletin/prevtrax.html?num=" + expect;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		List<LotteryOpenCode> tmplist = getBjkl8AndBjpk10(lotteryName, data);
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		for (LotteryOpenCode tmpBean : tmplist) {
// 			if(CodeValidate.isBjpk10(tmpBean.getCode())) {
// 				list.add(tmpBean);
// 			}
// 		}
// 		return list;
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getFc3dOpenCode(String lotteryName, String expect) {
// 		logger.info("获取福彩3d开奖号码....");
// 		String url = "http://www.bwlc.net/bulletin/prevpk3.html?num=20" + expect;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		List<LotteryOpenCode> tmplist = getFc3dCode(lotteryName, data);
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		for (LotteryOpenCode tmpBean : tmplist) {
// 			if(CodeValidate.is3d(tmpBean.getCode())) {
// 				list.add(tmpBean);
// 			}
// 		}
// 		return list;
// 	}
//
// 	public static void main(String[] args) {
// 		//List<LotteryOpenCode> codes = getBjkl8OpenCode("bjkl8", "680841");
// 		List<LotteryOpenCode> codes = getFc3dOpenCode("fc3d", "15088");
// 		for (LotteryOpenCode lotteryOpenCode : codes) {
// 			System.out.println(JSONObject.fromObject(lotteryOpenCode));
// 		}
// 	}
//
// 	private static List<LotteryOpenCode> getBjkl8AndBjpk10(String lotteryName, String data) {
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		if(StringUtil.isNotNull(data)) {
// 			try {
// 				String resource = data.replaceAll("(\t|\r|\n)", "");
// 				NodeList nList = HtmlUtils.FilterCss(resource, ".tb", "UTF-8");
// 				String currTime = DateUtil.getCurrentTime();
// 				if(nList.size() > 0) {
// 					NodeList tdList = HtmlUtils.FilterTag(nList.elementAt(0).toHtml(), "td", "UTF-8");
// 					Node[] nodes = tdList.toNodeArray();
// 					LotteryOpenCode lotteryOpenCode = new LotteryOpenCode();
// 					lotteryOpenCode.setLottery(lotteryName);
// 					lotteryOpenCode.setTime(currTime);
// 					lotteryOpenCode.setOpenStatus(0);
// 					for (int i = 0; i < nodes.length; i++) {
// 						if(nodes[i].getChildren() != null) {
// 							for (Node node : nodes[i].getChildren().toNodeArray()) {
// 								String value = node.toHtml();
// 								if(StringUtil.isNotNull(value)) {
// 									if(i == 0) {
// 										lotteryOpenCode.setExpect(value);
// 									}
// 									if(i == 1) {
// 										lotteryOpenCode.setCode(value);
// 									}
// 								}
// 							}
// 						}
// 					}
// 					if(lotteryOpenCode.getCode() != null) {
// 						list.add(lotteryOpenCode);
// 					}
// 				}
// 			} catch (Exception e) {
// 				logger.error("解析数据失败...", e);
// 			}
// 		}
// 		return list;
// 	}
//
// 	private static List<LotteryOpenCode> getFc3dCode(String lotteryName, String data) {
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		if(StringUtil.isNotNull(data)) {
// 			try {
// 				String resource = data.replaceAll("(\t|\r|\n)", "");
// 				NodeList nList = HtmlUtils.FilterCss(resource, ".tb", "UTF-8");
// 				String currTime = DateUtil.getCurrentTime();
// 				if(nList.size() > 0) {
// 					NodeList tdList = HtmlUtils.FilterTag(nList.elementAt(0).toHtml(), "td", "UTF-8");
// 					Node[] nodes = tdList.toNodeArray();
// 					LotteryOpenCode lotteryOpenCode = new LotteryOpenCode();
// 					lotteryOpenCode.setLottery(lotteryName);
// 					lotteryOpenCode.setTime(currTime);
// 					StringBuffer code = new StringBuffer();
// 					for (int i = 0; i < nodes.length; i++) {
// 						if(nodes[i].getChildren() != null) {
// 							for (Node node : nodes[i].getChildren().toNodeArray()) {
// 								String value = node.toHtml();
// 								if(StringUtil.isNotNull(value)) {
// 									if(i == 0) {
// 										lotteryOpenCode.setExpect(value.substring(2));
// 									}
// 									if(i == 1) {
// 										code.append(value + ",");
// 									}
// 									if(i == 2) {
// 										code.append(value + ",");
// 									}
// 									if(i == 3) {
// 										code.append(value);
// 									}
// 								}
// 							}
// 						}
// 					}
// 					lotteryOpenCode.setCode(code.toString());
// 					if(lotteryOpenCode.getCode() != null) {
// 						list.add(lotteryOpenCode);
// 					}
// 				}
// 			} catch (Exception e) {
// 				logger.error("解析数据失败...", e);
// 			}
// 		}
// 		return list;
// 	}
// }