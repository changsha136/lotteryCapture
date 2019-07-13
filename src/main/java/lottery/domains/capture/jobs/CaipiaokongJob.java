// package lottery.domains.capture.jobs;
//
// import com.alibaba.fastjson.JSON;
// import javautils.date.Moment;
// import javautils.http.HttpClientUtil;
// import lottery.domains.capture.sites.cpk.CPKBean;
// import lottery.domains.capture.utils.CodeValidate;
// import lottery.domains.capture.utils.ExpectValidate;
// import lottery.domains.content.biz.LotteryOpenCodeService;
// import lottery.domains.content.dao.LotteryCrawlerStatusDao;
// import lottery.domains.content.entity.LotteryOpenCode;
// import net.sf.json.JSONException;
// import org.apache.commons.lang.StringUtils;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Component;
//
// import java.util.*;
//
// /**
//  * 彩票控开奖源 注意频率，同一彩种间隔必须大于3秒，否则会封IP，该接口只能用于一个环境，否则会封IP
//  */
// @Component
// public class CaipiaokongJob {
// 	private static final String UID = "974888"; // 用户ID，购买后有  setyouridhere替换为自己的id
// 	private static final String TOKEN = "6739843043cc3046bc1b6ed01cf56a9e885dc300"; // token，购买后有 setyourtokenhere替换为自己的token
// 	private static final String NUM = "10"; // 每次获取条数 1－50
// 	private static final String URL = "http://api.kaijiangtong.com/lottery/?&format=json&uid=" + UID + "&token=" + TOKEN + "&num=" + NUM;
//
// 	// 彩票code列表
// 	private static final Map<String, String> LOTTERIES = new HashMap<>();
// 	static {
// 		LOTTERIES.put("cqssc", "cqssc"); // 重庆时时彩
// 		LOTTERIES.put("xjssc", "xjssc"); // 新疆时时彩
// 		LOTTERIES.put("tjssc", "tjssc"); // 天津时时彩
//// 		LOTTERIES.put("twbg", "tw5fc"); // 台湾5分彩/台湾快乐8
//// 		LOTTERIES.put("bjklb", "bj5fc"); // 北京5分彩/北京快乐8
// 		// LOTTERIES.put("jndklb", "jnd3d5fc"); // 加拿大3.5
//
//// 		LOTTERIES.put("gdsyxw", "gd11x5"); // 广东11选5
//// 		LOTTERIES.put("jxsyxw", "jx11x5"); // 江西11选5
//// 		LOTTERIES.put("ahsyxw", "ah11x5"); // 安徽11选5
//// 		LOTTERIES.put("shsyxw", "sh11x5"); // 上海11选5
//// 		LOTTERIES.put("sdsyydj", "sd11x5"); // 山东11选5
//
//// 		LOTTERIES.put("jsks", "jsk3"); // 江苏快3
//// 		LOTTERIES.put("gxks", "gxk3"); //广西快3
//// 		LOTTERIES.put("bjks", "bjk3"); //北京快3
//// 		LOTTERIES.put("hubks", "hbk3"); //湖北快3
//// 		LOTTERIES.put("hbks", "hebk3"); //河北快3
//// 		LOTTERIES.put("ahks", "ahk3"); // 安徽快3
//// 		LOTTERIES.put("shks", "shk3"); // 上海快3
//// 		LOTTERIES.put("gsks", "gsk3"); //甘肃快3
//// 		LOTTERIES.put("jxks", "jxk3"); //江西快3
//
// 		// LOTTERIES.put("jlks", "jlk3"); // 吉林快3
//
// 		LOTTERIES.put("sd", "fc3d"); // 福彩3D
// 		LOTTERIES.put("pls", "pl3"); // 排列三
//
// 		LOTTERIES.put("bjpks", "bjpk10"); // 北京PK10
// 	}
//
// 	private static final Logger logger = LoggerFactory.getLogger(CaipiaokongJob.class);
//
// 	@Autowired
// 	private LotteryOpenCodeService lotteryOpenCodeService;
//
// 	@Autowired
// 	private LotteryCrawlerStatusDao lotteryCrawlerStatusDao;
//
// 	private static boolean isRuning = false;
//
// 	@Scheduled(cron = "0,5,10,15,20,25,30,35,40,45,50,55 * 0-3,7-23 * * *") // 注意频率，同一彩种间隔必须大于3秒，否则会封IP，该接口只能用于一个环境，否则会封IP
// 	// @PostConstruct
// 	public void execute() {
// 		synchronized (CaipiaokongJob.class) {
// 			if (isRuning == true) {
// 				return;
// 			}
// 			isRuning = true;
// 		}
//
// 		try {
// 			logger.debug("开始抓取彩票控开奖数据>>>>>>>>>>>>>>>>");
//
// 			long start = System.currentTimeMillis();
// 			start();
// 			long spend = System.currentTimeMillis() - start;
//
// 			logger.debug("完成抓取彩票控开奖数据>>>>>>>>>>>>>>>>耗时{}", spend);
// 		} catch (Exception e) {
// 			logger.error("抓取彩票控开奖数据出错", e);
// 		} finally {
// 			isRuning = false;
// 		}
// 	}
//
// 	private void start() {
// 		for (String lottery : LOTTERIES.keySet()) {
// 			try {
// 				String result = getResult(lottery);
// 				handleData(lottery, result);
// 			} catch (Exception e) {
// 				logger.error("抓取彩票控"+lottery+"开奖数据出错", e);
// 			}
// 		}
// 	}
//
// 	public String getResult(String name) {
// 		String url = URL + "&name="+name+"&_=" + System.currentTimeMillis();
// 		System.out.println(url);
// 		String charset = "UTF-8";
// 		String result = get(url, charset);
// 		return result;
// 	}
//
// 	@SuppressWarnings("rawtypes")
// 	private void handleData(String name, String result) {
// 		if (StringUtils.isEmpty(result)) {
// 			return;
// 		}
//
// 		HashMap<String, Object> hashMap = JSON.parseObject(result, HashMap.class);
// 		if (hashMap == null) {
// 			return;
// 		}
//
// 		List<CPKBean> list = new ArrayList<>();
//
// 		try {
// 			Set<String> set = hashMap.keySet();
// 			Iterator<String> iterator = set.iterator();
// 			while(iterator.hasNext()) {
// 				String expect = iterator.next();
// 				String value = hashMap.get(expect).toString();
// 				CPKBean cpkBean = JSON.parseObject(value, CPKBean.class);
// 				cpkBean.setExpect(expect);
// 				list.add(cpkBean);
// 			}
// 		} catch (JSONException e) {
// 			logger.error("获取彩票控" + name +"数据时出错", e);
// 		}
//
// 		// 处理数据
// 		for (CPKBean bean : list) {
// 			handleBean(name, bean);
// 		}
// 	}
//
// 	private boolean handleBean(String name, CPKBean bean) {
// 		String expect = null;
// 		String date = null;
// 		String code = null;
// 		String realName = LOTTERIES.get(name);
// 		switch (realName) {
// 			// 时时彩
// 			case "cqssc":
// 			case "xjssc":
// 			case "tjssc":
// 			case "sd11x5":
// 			case "jx11x5":
// 			case "sh11x5":
// 			case "ahk3":
// 			case "hbk3":
// 			case "hebk3":
// 				date = bean.getExpect().substring(0, 8);
// 				expect = bean.getExpect().substring(8);
// 				if (expect.length() == 2) {
// 					expect = "0" + expect;
// 				}
// 				expect = date + "-" + expect;
// 				code = bean.getNumber();
// 				break;
// 			// 11选5
// 			case "ah11x5":
// 			case "gd11x5":
// 			case "jsk3":
// 			case "jlk3":
// 			case "shk3":
// 			case "gxk3":
// 			case "gsk3":
// 			case "jxk3":
// 				date = bean.getExpect().substring(0, 6);
// 				if (date.length() == 6) {
// 					date = "20" + date;
// 				}
// 				expect = bean.getExpect().substring(6);
// 				if (expect.length() == 2) {
// 					expect = "0" + expect;
// 				}
// 				expect = date + "-" + expect;
// 				code = bean.getNumber();
// 				break;
// 			// 低频彩
// 			case "fc3d":
// 			case "pl3":
// 				expect = bean.getExpect().substring(2);
// 				code = bean.getNumber();
// 				break;
// 			case "bj5fc":
// 				expect = bean.getExpect();
// 				code = bean.getNumber().substring(0, 59);
// 				code = convertFFCCode(code);
// 				break;
// 			case "bjpk10":
// 			case "bjk3":
// 				expect = bean.getExpect();
// 				code = bean.getNumber();
// 				break;
// 			// 台湾5分彩
// 			case "tw5fc":
// 			case "jnd3d5fc":
// 				expect = bean.getExpect();
// 				code = bean.getNumber().substring(0, 59);
// 				code = convertFFCCode(code);
// 				break;
// 			default:
// 				break;
// 		}
//
// 		if (expect == null || code == null) {
// 			return false;
// 		}
//
// 		if (CodeValidate.validate(realName, code) == false) {
// 			logger.error("彩票控" + realName + "抓取号码" + code + "错误");
// 			return false;
// 		}
//
// 		if (ExpectValidate.validate(realName, expect) == false) {
// 			logger.error("彩票控" + realName + "抓取期数" + expect + "错误");
// 			return false;
// 		}
//
// 		LotteryOpenCode lotteryOpenCode = new LotteryOpenCode(realName, expect, code, new Moment().toSimpleTime(), 0, null, "CPK");
// 		if (StringUtils.isNotEmpty(bean.getDateline())) {
// 			lotteryOpenCode.setInterfaceTime(bean.getDateline());
// 		}
// 		else {
// 			lotteryOpenCode.setInterfaceTime(new Moment().toSimpleTime());
// 		}
//
// 		boolean added = lotteryOpenCodeService.add(lotteryOpenCode, false);
// 		if (added) {
// 			if ("bj5fc".equals(realName)) {
// 				LotteryOpenCode bjkl8Code = new LotteryOpenCode("bjkl8", expect, bean.getNumber().substring(0, 59), new Moment().toSimpleTime(), 0, null, "CPK");
// 				bjkl8Code.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
// 				lotteryOpenCodeService.add(bjkl8Code, false);
// 			}
// 		}
//
// 		return added;
// 	}
//
// 	public static String get(String urlAll, String charset) {
// 		try {
// 			String result = HttpClientUtil.get(urlAll, null, 30000);
// 			return result;
// 		} catch (Exception e) {
// 			logger.error("请求彩票控出错", e);
// 			return null;
// 		}
// 	}
//
//
// 	/**
// 	 * 转换1.5分彩号码
// 	 */
// 	private String convertFFCCode(String openCode) {
// 		String[] codes = openCode.split(",");
// 		String code1 = mergeFFCCode(codes[0], codes[1], codes[2], codes[3]);
// 		String code2 = mergeFFCCode(codes[4], codes[5], codes[6], codes[7]);
// 		String code3 = mergeFFCCode(codes[8], codes[9], codes[10], codes[11]);
// 		String code4 = mergeFFCCode(codes[12], codes[13], codes[14], codes[15]);
// 		String code5 = mergeFFCCode(codes[16], codes[17], codes[18], codes[19]);
//
// 		return code1 + "," + code2 + "," + code3 + "," + code4 + "," + code5;
// 	}
// 	private String mergeFFCCode(String code1, String code2, String code3, String code4) {
// 		int codeInt1 = Integer.valueOf(code1);
// 		int codeInt2 = Integer.valueOf(code2);
// 		int codeInt3 = Integer.valueOf(code3);
// 		int codeInt4 = Integer.valueOf(code4);
//
// 		int codeSum = codeInt1 + codeInt2 + codeInt3 + codeInt4;
// 		String codeSumStr = codeSum+"";
// 		String finalCode = codeSumStr.substring(codeSumStr.length() - 1);
// 		return finalCode;
// 	}
// }