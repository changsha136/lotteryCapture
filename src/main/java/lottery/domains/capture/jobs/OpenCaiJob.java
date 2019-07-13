//package lottery.domains.capture.jobs;
//
//import javautils.StringUtil;
//import javautils.date.Moment;
//import javautils.http.HttpClientUtil;
//import lottery.domains.capture.sites.opencai.OpenCaiBean;
//import lottery.domains.capture.utils.CodeValidate;
//import lottery.domains.capture.utils.ExpectValidate;
//import lottery.domains.content.biz.LotteryOpenCodeService;
//import lottery.domains.content.entity.LotteryOpenCode;
//import net.sf.json.JSONArray;
//import net.sf.json.JSONObject;
//import org.apache.commons.lang.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//import java.util.*;
//
///**
// * 时时彩 注意间隔，不允许有多线程 否则会封IP
// */
//@Component
//public class OpenCaiJob {
//	private static final Logger logger = LoggerFactory.getLogger(OpenCaiJob.class);
//	//TC4EB799E17520495K
//	//tf71d39644f573993k
////	tc4eb799e17520495k
//	private static final String SUB_URL = "/newly.do?token=T2AD09FC355AD9BE9K&rows=5&format=json"; // setyourtokenhere替换为自己的token
//// 	private static final String SUB_URL = "/newly.do?token=TC4EB799E17520495K&rows=5&format=json"; // setyourtokenhere替换为自己的token
////	private static final String[] SITES = {"http://z.apiplus.net", "http://a.apiplus.net", "http://e.apiplus.net", "http://b.apiplus.net", "http://c.apiplus.net", "http://d.apiplus.net"};
//	private static final String[] SITES = {"http://b.apiplus.net", "http://z.apiplus.net"};
////	private static final String[] SITES = {"http://z.apiplus.net", "http://a.apiplus.net", "http://e.apiplus.net", "http://b.apiplus.net", "http://c.apiplus.net", "http://d.apiplus.net"};
////	private static final String[] SITES = {"http://xx.xx.xx.xx:xxxx", "http://z.apiplus.net", "http://a.apiplus.net", "http://e.apiplus.net", "http://b.apiplus.net", "http://c.apiplus.net", "http://d.apiplus.net"};
//	// http://xx.xx.xx.xx:xxxx替换为自己的VIP服务器和端口，后面的z.apiplus.net/a.apiplus.net等不需要动
//
//	@Autowired
//	private LotteryOpenCodeService lotteryOpenCodeService;
//
//	private static boolean isRuning = false;
//
//	// @Scheduled(cron = "0/20 * * * * *") // 注意间隔，一秒钟最多50次，不允许有多线程 否则会封IP
//	 @Scheduled(cron = "0/5 * * * * *") // 注意间隔，一秒钟最多50次，不允许有多线程 否则会封IP
////	@Scheduled(cron = "0/3 * * * * *") // 注意间隔，一秒钟最多50次，不允许有多线程 否则会封IP
//	public void execute() {
//		synchronized (OpenCaiJob.class) {
//			if (isRuning == true) {
//				return;
//			}
//			isRuning = true;
//		}
//
//		try {
//			start();
//		} catch (Exception e) {
//			logger.error("抓取OpenCai开奖数据出错", e);
//		} finally {
//			isRuning = false;
//		}
//	}
//
//	public void start() {
//		for (int i=0; i<SITES.length; i++) {
//			String site = SITES[i];
//			logger.debug("开始抓取OpenCai[{}]开奖数据", site);
//			long start = System.currentTimeMillis();
//
//			String url = site + SUB_URL + "&_=" + System.currentTimeMillis();
//
//			String result = getHttpResult(url);
//
//			boolean succeed = false;
//			if (StringUtils.isNotEmpty(result)) {
//				succeed = handleData(result, site);
//			}
//
//			long spend = System.currentTimeMillis() - start;
//			if (succeed) {
//				logger.debug("成功抓取OpenCai[{}]开奖数据，并处理成功，耗时{}", site, spend);
//				break;
//			}
//			else {
//				logger.warn("完成抓取OpenCai[{}]开奖数据，但处理失败，耗时{}", site, spend);
//			}
//		}
//	}
//
//	private boolean handleData(String data, String site) {
//		try {
//			if (StringUtil.isNotNull(data)) {
//				Object object = JSONObject.fromObject(data).get("data");
//				String code = (String) JSONObject.fromObject(data).get("code");
//				//{"rows":5,"code":"sd11x5","remain":"139hrs","data":[{"expect":"2019021743","opencode":"09,03,04,01,07","opentime":"2019-02-17 23:02:30","opentimestamp":1550415750},{"expect":"2019021742","opencode":"05,07,08,11,04","opentime":"2019-02-17 22:42:29","opentimestamp":1550414549},{"expect":"2019021741","opencode":"03,05,08,11,07","opentime":"2019-02-17 22:22:26","opentimestamp":1550413346},{"expect":"2019021740","opencode":"03,02,01,10,07","opentime":"2019-02-17 22:02:26","opentimestamp":1550412146},{"expect":"2019021739","opencode":"05,01,11,07,09","opentime":"2019-02-17 21:42:14","opentimestamp":1550410934}]}
//				JSONArray array = JSONArray.fromObject(object);
//				List<OpenCaiBean> list = new ArrayList<OpenCaiBean>();
//				for (Iterator iter = array.iterator(); iter.hasNext();) {
//					JSONObject jsonObject = (JSONObject) iter.next();
//					OpenCaiBean bean = (OpenCaiBean) JSONObject.toBean(jsonObject,
//							OpenCaiBean.class);
//					if(StringUtils.isNotEmpty(code)){
//						bean.setCode(code);
//					}
//					list.add(bean);
//				}
//				Collections.reverse(list);
//				for (OpenCaiBean bean : list) {
//					handleOpenCaiBean(bean, site);
//				}
//
//				return true;
//			}
//			return false;
//		} catch (Exception e) {
//			logger.error("解析OpenCai数据出错：" + data + "，URL：" + site, e);
//			return false;
//		}
//	}
//
//	private boolean handleOpenCaiBean(OpenCaiBean bean, String site) {
//		LotteryOpenCode lotteryOpenCode = null;
//		String expect;
//		switch (bean.getCode()) {
//			// 时时彩
////			case "cqssc":
//			case "jxssc":
//			case "xjssc":
//			case "tjssc":
//				expect = bean.getExpect();
//				expect = expect.substring(0, 8) + "-" + expect.substring(8);
//				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), expect,
//						bean.getOpencode(), bean.getOpentime(), 0);
//				break;
//			// 11选5
//			case "ah11x5":
//			case "gd11x5":
//			case "sd11x5":
//			case "sh11x5":
//			case "jx11x5":
//				expect = bean.getExpect();
//				expect = expect.substring(0, 8) + "-0" + expect.substring(8);
//				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), expect,
//						bean.getOpencode(), bean.getOpentime(), 0);
//				break;
//			// 低频彩
////			case "fc3d":
////			case "pl3":
////				expect = bean.getExpect();
////				expect = expect.substring(2, 7);
////				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), expect,
////						bean.getOpencode(), bean.getOpentime(), 0);
////				break;
//			// 快三
//			case "ahk3":
//			case "jsk3":
//			case "jlk3":
//			case "hubk3":
//			case "shk3":
//			case "gxk3":
//			case "hebk3":
//				if (bean.getCode().equals("hubk3")) {
//					bean.setCode("hbk3");
//				}
//				expect = bean.getExpect();
//				expect = expect.substring(0, 8) + "-" + expect.substring(8);
//				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), expect,
//						bean.getOpencode(), bean.getOpentime(), 0);
//				break;
//			case "bjkl8":
//				String openCode = bean.getOpencode().split("\\+")[0];
//				String bjkl8Code = convertFFCCode(openCode);
//				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), bean.getExpect(),
//						bjkl8Code, bean.getOpentime(), 0);
//				lotteryOpenCode.setLottery("bj5fc");
//				break;
////			case "bjpk10":
////				expect = bean.getExpect();
////				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), expect,
////						bean.getOpencode(), bean.getOpentime(), 0);
////				break;
//			// // 韩国1.5分彩
//			// case "krkeno":
//			// 	// 07,14,18,19,20,22,25,29,33,34,36,40,46,51,56,61,67,71,75,80
//			// 	String krkenoCode = convertFFCCode(bean.getOpencode());
//			// 	lotteryOpenCode = new LotteryOpenCode(bean.getCode(), bean.getExpect(),
//			// 			krkenoCode, bean.getOpentime(), 0);
//			// 	lotteryOpenCode.setLottery("hgffc");//项目使用code为 hgffc
//			// 	break;
//			// // 东京1.5分彩
//			// case "jpkeno":
//			// 	// 4,14,18,21,28,34,35,37,39,44,45,46,47,50,54,55,65,72,77,79
//			// 	String jpkenoCode = convertFFCCode(bean.getOpencode());
//			// 	expect = bean.getExpect();
//			// 	expect = expect.substring(0, 8) + "-" + expect.substring(8);
//			// 	lotteryOpenCode = new LotteryOpenCode(bean.getCode(), expect,
//			// 			jpkenoCode, bean.getOpentime(), 0);
//			// 	lotteryOpenCode.setLottery("djffc");
//			// 	break;
//			// 台湾5分彩
//			case "twbingo":
//				openCode = bean.getOpencode().split("\\+")[0];
//				String twbingoCode = convertFFCCode(openCode);
//				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), bean.getExpect(),
//						twbingoCode, bean.getOpentime(), 0);
//				lotteryOpenCode.setLottery("tw5fc");
//				break;
//			// 加拿大3.5
//			case "cakeno":
//				openCode = bean.getOpencode().substring(0, 59);
//				String cakenoCode = convertFFCCode(openCode);
//				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), bean.getExpect(),
//						cakenoCode, bean.getOpentime(), 0);
//				lotteryOpenCode.setLottery("jnd3d5fc");
//				break;
//			// // 新加坡2分彩
//			// case "sgkeno":
//			// 	String sgkenoCode = convertFFCCode(bean.getOpencode());
//			// 	lotteryOpenCode = new LotteryOpenCode(bean.getCode(), bean.getExpect(),
//			// 			sgkenoCode, bean.getOpentime(), 0);
//			// 	lotteryOpenCode.setLottery("xjp2fc");
//			// 	break;
//			case "shssl":
//				expect = bean.getExpect();
//				expect = expect.substring(0, 8) + "-" + expect.substring(8);
//				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), expect,
//						bean.getOpencode(), bean.getOpentime(), 0);
//				break;
//			case "pl5":
//				expect = bean.getExpect();
//				lotteryOpenCode = new LotteryOpenCode(bean.getCode(), expect,
//						bean.getOpencode(), bean.getOpentime(), 0);
//				break;
//			default:
//				break;
//		}
//
//		if (lotteryOpenCode == null) {
//			return false;
//		}
//
//		if (CodeValidate.validate(lotteryOpenCode.getLottery(), lotteryOpenCode.getCode()) == false) {
//			logger.error("开彩网" + lotteryOpenCode.getLottery() + "抓取号码" + lotteryOpenCode.getCode() + "错误");
//			return false;
//		}
//
//		if (ExpectValidate.validate(lotteryOpenCode.getLottery(), lotteryOpenCode.getExpect()) == false) {
//			logger.error("开彩网" + lotteryOpenCode.getLottery() + "抓取期数" + lotteryOpenCode.getExpect() + "错误");
//			return false;
//		}
//
//		lotteryOpenCode.setRemarks(site);
//		lotteryOpenCode.setTime(new Moment().toSimpleTime());
//		if (StringUtils.isNotEmpty(bean.getOpentime())) {
//			lotteryOpenCode.setInterfaceTime(bean.getOpentime());
//		}
//		else {
//			lotteryOpenCode.setInterfaceTime(new Moment().toSimpleTime());
//		}
//
//		boolean add = lotteryOpenCodeService.add(lotteryOpenCode, false);
//		if (add) {
//			// 北京快乐8
//			if ("bj5fc".equals(lotteryOpenCode.getLottery())) {
//				LotteryOpenCode bjkl8Code = new LotteryOpenCode("bjkl8", lotteryOpenCode.getExpect(), bean.getOpencode().split("\\+")[0], new Moment().toSimpleTime(), 0, null, site);
//				bjkl8Code.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
//				lotteryOpenCodeService.add(bjkl8Code, false);
//			}
//		}
//		return add;
//	}
//
//	/**
//	 * 获取数据
//	 */
//	public static String getHttpResult(String url) {
//		try {
//			// logger.debug("开始请求OpenCai，URL：{}", url);
//
//			// 设置头
//			Map<String, String> header = new HashMap<>();
//			header.put("referer", "http://www.baidu.com/");
//			header.put("user-agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36");
//
//			String data = HttpClientUtil.post(url, null, header, 10000);
//			return data;
//		} catch (Exception e) {
//			logger.error("请求OpenCai出错,URL：" + url, e);
//			return null;
//		}
//	}
//
//	/**
//	 * 转换1.5分彩号码
//	 */
//	private String convertFFCCode(String openCode) {
//		String[] codes = openCode.split(",");
//		String code1 = mergeFFCCode(codes[0], codes[1], codes[2], codes[3]);
//		String code2 = mergeFFCCode(codes[4], codes[5], codes[6], codes[7]);
//		String code3 = mergeFFCCode(codes[8], codes[9], codes[10], codes[11]);
//		String code4 = mergeFFCCode(codes[12], codes[13], codes[14], codes[15]);
//		String code5 = mergeFFCCode(codes[16], codes[17], codes[18], codes[19]);
//
//		return code1 + "," + code2 + "," + code3 + "," + code4 + "," + code5;
//	}
//	private String mergeFFCCode(String code1, String code2, String code3, String code4) {
//		int codeInt1 = Integer.valueOf(code1);
//		int codeInt2 = Integer.valueOf(code2);
//		int codeInt3 = Integer.valueOf(code3);
//		int codeInt4 = Integer.valueOf(code4);
//
//		int codeSum = codeInt1 + codeInt2 + codeInt3 + codeInt4;
//		String codeSumStr = codeSum+"";
//		String finalCode = codeSumStr.substring(codeSumStr.length() - 1);
//		return finalCode;
//	}
//}