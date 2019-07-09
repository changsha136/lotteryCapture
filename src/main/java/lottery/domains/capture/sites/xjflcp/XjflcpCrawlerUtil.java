// package lottery.domains.capture.sites.xjflcp;
//
// import javautils.StringUtil;
// import javautils.date.DateUtil;
// import javautils.html.HtmlUtils;
// import javautils.http.EasyHttpClient;
// import lottery.domains.capture.utils.CodeValidate;
// import lottery.domains.content.entity.LotteryOpenCode;
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
// public class XjflcpCrawlerUtil {
//
// 	private static final Logger logger = LoggerFactory.getLogger(XjflcpCrawlerUtil.class);
// 	private static Map<String, EasyHttpClient> httpClientPool = new HashMap<>();
//
// 	private static EasyHttpClient getHttpClient(String lotteryName) {
// 		if (!httpClientPool.containsKey(lotteryName)) {
// 			httpClientPool.put(lotteryName, new EasyHttpClient());
// 		}
// 		return httpClientPool.get(lotteryName);
// 	}
//
// 	private XjflcpCrawlerUtil() {
//
// 	}
//
// 	public static synchronized List<LotteryOpenCode> getXjsscOpenCode(String lotteryName, String expect) {
// 		logger.info("获取新疆时时彩开奖号码....");
// 		String[] tmpArray = expect.split("-");
// 		String expectDate = tmpArray[0];
// 		String expectNumber = String.format("%02d", Integer.parseInt(tmpArray[1]));
// 		String targetExpect = expectDate + expectNumber;
// 		String url = "http://www.xjflcp.com/video/prizeDetail.do?operator=detailssc&lotterydraw=" + targetExpect;
// 		EasyHttpClient httpClient = getHttpClient(lotteryName);
// 		String data = httpClient.get(url);
// 		List<LotteryOpenCode> tmplist =  getOpenCode(lotteryName, data);
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		for (LotteryOpenCode tmpBean : tmplist) {
// 			if(CodeValidate.isSsc(tmpBean.getCode())) {
// 				list.add(tmpBean);
// 			}
// 		}
// 		return list;
// 	}
//
// 	private static List<LotteryOpenCode> getOpenCode(String lotteryName, String data) {
// 		List<LotteryOpenCode> list = new ArrayList<>();
// 		if(StringUtil.isNotNull(data)) {
// 			try {
// 				String resource = data.replaceAll("(\t|\r|\n)", "");
// 				NodeList nList = HtmlUtils.FilterCss(resource, ".r5", "UTF-8");
// 				String currTime = DateUtil.getCurrentTime();
// 				if(nList.size() > 0) {
// 					NodeList tdList = HtmlUtils.FilterTag(nList.elementAt(0).toHtml(), "td", "UTF-8");
// 					Node[] nodes = tdList.toNodeArray();
// 					LotteryOpenCode lotteryOpenCode = new LotteryOpenCode();
// 					lotteryOpenCode.setLottery(lotteryName);
// 					lotteryOpenCode.setTime(currTime);
// 					for (int i = 0; i < nodes.length; i++) {
// 						if(nodes[i].getChildren() != null) {
// 							for (Node node : nodes[i].getChildren().toNodeArray()) {
// 								String html = node.toHtml();
// 								if(StringUtil.isNotNull(html)) {
// 									if(i == 1) {
// 										String expectDate =  html.substring(0, 8);
// 										String expectNumber = String.format("%03d", Integer.parseInt(html.substring(8)));
// 										String expect = expectDate + "-" + expectNumber;
// 										lotteryOpenCode.setExpect(expect);
// 									}
// 									if(i == 5) {
// 										String code = html.replace("&nbsp;", ",");
// 										if(!",,,,".equals(code)) {
// 											lotteryOpenCode.setCode(code);
// 										}
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
// }