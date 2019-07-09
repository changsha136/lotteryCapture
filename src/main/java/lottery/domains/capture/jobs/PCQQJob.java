//package lottery.domains.capture.jobs;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//import org.apache.commons.lang.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//import com.alibaba.fastjson.JSON;
//
//import javautils.date.Moment;
//import javautils.http.HttpClientUtil;
//import lottery.domains.capture.sites.pcqq.PCQQBean;
//import lottery.domains.capture.utils.CodeValidate;
//import lottery.domains.capture.utils.ExpectValidate;
//import lottery.domains.content.biz.LotteryOpenCodeService;
//import lottery.domains.content.entity.LotteryOpenCode;
//
///**
// * 腾讯分分彩，官网抓取，不需要修改，但必须保证抓取服务器时间和腾讯服务器时间间隔在5秒之内，统一采用东8区时间即可 
// * 已经无效了
// */
//@Component
//public class PCQQJob {
//	private static final String ONLINE_URL = "https://mma.qq.com/cgi-bin/im/online"; // PCQQ在线人数
//	private static final String CGI_SVRTIME_URL = "http://cgi.im.qq.com/cgi-bin/cgi_svrtime"; // 腾讯服务器时间
//
//	private static final Logger logger = LoggerFactory.getLogger(PCQQJob.class);
//	private static final String NAME = "txffc";
//
//	private static ConcurrentHashMap<String, String> HIS_OPEN_CODES = new ConcurrentHashMap<>(); // 历史开奖号码，只存当天的数据
//
//	@Autowired
//	private LotteryOpenCodeService lotteryOpenCodeService;
//
//	private static boolean isRuning = false;
//
//	@Scheduled(cron = "8,15,20,25,30,35 * * * * *")
//	// @Scheduled(cron = "0/3 * * * * *")
//	// @PostConstruct
//	public void schedule() {
//		synchronized (PCQQJob.class) {
//			if (isRuning == true) {
//				return;
//			}
//			isRuning = true;
//		}
//
//		try {
//
//			logger.debug("开始抓取PCQQ官网在线人数数据>>>>>>>>>>>>>>>>" );
//
//			long start = System.currentTimeMillis();
//
//			start();
//			long spend = System.currentTimeMillis() - start;
//
//			logger.debug("完成抓取PCQQ官网在线人数数据>>>>>>>>>>>>>>>>耗时{}", spend);
//		} catch (Exception e) {
//			logger.error("抓取PCQQ官网在线人数数据出错", e);
//		} finally {
//			isRuning = false;
//		}
//	}
//
//	private void start() {
//		int startMinute = new Moment().get("minute"); // 开始抓取的分钟
//
//		// // 验证本地时间和腾讯服务器时间，如果相差大于4秒，则不处理数据
//		// boolean validateTime = validateTencentServerTime();
//		// if (validateTime == false) {
//		// 	return;
//		// }
//
//		String result = getOnlineNum();
//
//		int endMinute = new Moment().get("minute"); // 结束抓取的分钟
//		if (startMinute != endMinute) {
//			logger.error("PCQQ官网在线人数抓取超时，开始分钟{}，结束分钟{}", startMinute, endMinute);
//			return;
//		}
//
//		int endSecond = new Moment().get("second");
//		if (endSecond >= 40) {
//			logger.error("PCQQ官网在线人数抓取不处理，因为处理完成的秒数{}>=40秒，可能是下一期的数据", endSecond);
//			return;
//		}
//
//		handleData(result);
//	}
//
//	private boolean validateTencentServerTime() {
//		Moment tencentServerTime = getTencentServerTime();
//		if (tencentServerTime == null) {
//			return false;
//		}
//		Moment now = new Moment();
//
//		int diffSeconds = now.difference(tencentServerTime, "second");
//
//		if (diffSeconds >= 5 || diffSeconds <= -5) {
//			logger.error("当前服务器时间超过腾讯服务器时间太多，抓取服务器时间{}，PCQQ官网时间{}，本次不处理", now.toSimpleTime(), tencentServerTime.toSimpleTime());
//			return false;
//		}
//
//		logger.error("抓取服务器时间{}，PCQQ官网时间{}", now.toSimpleTime(), tencentServerTime.toSimpleTime());
//		return true;
//	}
//
//	private void handleData(String result) {
//		if (StringUtils.isEmpty(result)) {
//			return;
//		}
//
//		PCQQBean pcqqBean = JSON.parseObject(result, PCQQBean.class);
//		if (pcqqBean == null) {
//			logger.error("解析PCQQ返回数据出错，返回数据" + result);
//			return;
//		}
//
//		// 处理数据
//		handleBean(pcqqBean);
//	}
//
//	private boolean handleBean(PCQQBean bean) {
//		boolean valid = checkData(bean);
//		if (!valid) {
//			return false;
//		}
//
//		// 当期期号
//		String currentExpect = getExpectByTime(bean.getOnlinetime());
//		if (ExpectValidate.validate(NAME, currentExpect) == false) {
//			logger.error("PCQQ官网腾讯分分彩抓取期数" + currentExpect + "错误");
//			return false;
//		}
//
//		// 如果已经有本期的数据了，则不再处理
//		if (HIS_OPEN_CODES.containsKey(currentExpect)) {
//			return false;
//		}
//
//		// 算出当前期开奖号码
//		String currentCode = convertCode(bean.getOnlinenumber());
//		if (CodeValidate.validate(NAME, currentCode) == false) {
//			logger.error("PCQQ官网腾讯分分彩抓取号码" + currentCode + "错误");
//			return false;
//		}
//
//		// 验证这一期是否已经抓取过了
//		LotteryOpenCode dbData = lotteryOpenCodeService.get(NAME, currentExpect);
//		if (dbData != null) {
//			HIS_OPEN_CODES.put(currentExpect, dbData.getCode());
//
//			if (!dbData.getCode().equals(currentCode)) {
//				logger.error("PCQQ官网抓取时遇到错误：抓取{}期开奖号码{}与数据库已有开奖号码{}不符", currentExpect, currentCode, dbData.getCode());
//				return false;
//			}
//
//			return true;
//		}
//
//		// 抓取回来的数据放到本地缓存里
//		HIS_OPEN_CODES.put(currentExpect, currentCode);
//
//		// 上期期号
//		String lastExpectTime = new Moment().fromTime(bean.getOnlinetime()).subtract(1, "minutes").toSimpleTime();
//		String lastExpect = getExpectByTime(lastExpectTime);
//
//		if (!HIS_OPEN_CODES.containsKey(lastExpect)) {
//			LotteryOpenCode lastExpectCode = lotteryOpenCodeService.get(NAME, lastExpect);
//			if (lastExpectCode == null) {
//				logger.warn("PCQQ官网抓取时没有获取到上期"+lastExpect+"的开奖数据，本次不处理，本期：" + currentExpect);
//				return false;
//			}
//
//			// 上期的号码放到本地缓存里
//			HIS_OPEN_CODES.put(lastExpect, lastExpectCode.getCode());
//		}
//
//		// 上期开奖号码
//		String lastExpectCode = HIS_OPEN_CODES.get(lastExpect);
//		if (StringUtils.isEmpty(lastExpectCode)) {
//			return false;
//		}
//
//		// 如果本期和上奖开奖号码相同，那么把开奖号码状态改为无效撤单
//		int status = 0;
//		if (lastExpectCode.equals(currentCode)) {
//			// 0：待开奖；1：已开奖；2：无效待撤单；3：无效已撤单
//			status = 2;
//		}
//
//		LotteryOpenCode lotteryOpenCode = new LotteryOpenCode(NAME, currentExpect, currentCode, new Moment().toSimpleTime(), status, null, "PCQQ");
//		lotteryOpenCode.setInterfaceTime(bean.getOnlinetime());
//
//		boolean added = lotteryOpenCodeService.add(lotteryOpenCode, false);
//
//		if (added) {
//			logger.info("PCQQ官网成功抓取腾讯分分彩{}期开奖号码{}，上期{}开奖号码{}，是否自动撤单：{}", currentExpect, currentCode, lastExpect, lastExpectCode, status == 2 ? "是" : "否");
//
//			// 腾讯龙虎斗
//			if ("txffc".equals(NAME)) {
//				LotteryOpenCode txlhdCode = new LotteryOpenCode("txlhd", lotteryOpenCode.getExpect(), lotteryOpenCode.getCode(), lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(), null, lotteryOpenCode.getRemarks());
//				txlhdCode.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
//				lotteryOpenCodeService.add(txlhdCode, false);
//			}
//		}
//
//		return added;
//	}
//
//	/**
//	 * 获取腾讯服务器时间
//	 */
//	public static Moment getTencentServerTime() {
//		try {
//			String url = CGI_SVRTIME_URL + "?_=" + System.currentTimeMillis();
//
//			String data = getHttpResult(url);
//			if (data != null) {
//				Moment moment = new Moment().fromTime(data);
//				return moment;
//			}
//			return null;
//		} catch (Exception e) {
//			logger.error("获取PCQQ官网服务器时间出错", e);
//			return null;
//		}
//	}
//
//	/**
//	 * 获取在线人数
//	 */
//	public static String getOnlineNum() {
//		try {
//			String url = ONLINE_URL + "?_=" + System.currentTimeMillis();
//
//			String data = getHttpResult(url);
//			if (data != null && data.indexOf("online_resp") > -1) {
//				data = data.substring(12);
//				data = data.substring(0, data.length() -1);
//				return data;
//			}
//			return null;
//		} catch (Exception e) {
//			logger.error("获取PCQQ官网在线人数出错", e);
//			return null;
//		}
//	}
//
//	/**
//	 * 获取腾讯数据
//	 */
//	public static String getHttpResult(String url) {
//		try {
//			// 设置头
//			Map<String, String> header = new HashMap<>();
//			header.put("referer", "http://im.qq.com/pcqq/");
//			header.put("user-agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36");
//
//			String data = HttpClientUtil.post(url, null, header, 10000);
//			return data;
//		} catch (Exception e) {
//			logger.error("请求PCQQ官网出错", e);
//			return null;
//		}
//	}
//
//
//	private boolean checkData(PCQQBean bean) {
//		if (bean == null) {
//			logger.error("PCQQ官网数据非法，空数据");
//			return false;
//		}
//		if (StringUtils.isEmpty(bean.getOnlinetime()) || bean.getOnlinenumber() <= 0) {
//			logger.error("PCQQ官网数据非法:" + JSON.toJSONString(bean));
//			return false;
//		}
//
//		// 最小要1000才能组成开奖号码，否则是会出错的
//		if (bean.getOnlinenumber() < 1000) {
//			logger.error("PCQQ官网数据非法:" + JSON.toJSONString(bean));
//			return false;
//		}
//
//		return true;
//	}
//	/**
//	 *
//	 * 转换号码
//	 * 万位：onlinenumber所有数值相加取尾数
//	 * 千位：onlinenumber倒数第4位
//	 * 百位：onlinenumber倒数第3位
//	 * 十位：onlinenumber倒数第2位
//	 * 个位：onlinenumber倒数第1位
//	 */
//	private String convertCode(int onlinenumber) {
//		String[] chars = (onlinenumber+"").split("");
//		int sum = 0;
//		for (String aChar : chars) {
//			if (aChar != null && !"".equals(aChar)) {
//				sum += (Integer.valueOf(aChar));
//			}
//		}
//
//		// 万位
//		String wan = sum + "";
//		wan = wan.substring(wan.length() - 1);
//
//		String qian = chars[chars.length - 4] + "";
//		String bai = chars[chars.length - 3] + "";
//		String shi = chars[chars.length - 2] + "";
//		String ge = chars[chars.length - 1] + "";
//
//		return wan + "," + qian + "," + bai + "," + shi + "," + ge;
//	}
//
//	private static String getExpectByTime(String time) {
//		Moment moment = new Moment().fromTime(time);
//		int hour = moment.get("hour");
//		int minute = moment.get("minute");
//
//		if (hour == 0 && minute == 0) {
//			// 如果是0点0分，那么就是昨天的最后一期，即1440期
//			moment = moment.add(-1, "minutes");
//			hour = 24;
//		}
//		String date = moment.format("yyyyMMdd");
//		int dayExpect = (hour * 60) + minute;
//		String expect = date + "-" + (String.format("%04d", dayExpect));
//		return expect;
//	}
//
//	public static void main(String[] args) {
//		String time = "2017-09-18 00:01:59";
//		String curExpect = getExpectByTime(time);
//		System.out.println(curExpect);
//
//		String lastExpectTime = new Moment().fromTime(time).subtract(1, "minutes").toSimpleTime();
//		String lastExpect = getExpectByTime(lastExpectTime);
//
//		System.out.println(lastExpect);
//
//
//		// String url = ONLINE_URL + "?_=" + System.currentTimeMillis();
//		// System.out.println(url);
//		// String data = getData(url);
//		// System.out.println(data);
//
//		// System.out.println(convertCode2(214616782));
//
//		// Moment moment = new Moment().fromTime("2017-08-13 00:02:00");
//		// int hour = moment.get("hour");
//		// int minute = moment.get("minute");
//		//
//		// if (hour == 0 && minute == 0) {
//		// 	// 如果是0点0分，那么就是昨天的最后一期，即1440期
//		// 	moment = moment.add(-1, "minutes");
//		// 	hour = 24;
//		// }
//		// String date = moment.format("yyyyMMdd");
//		// // hour = moment.get("hour");
//		// // minute = moment.get("minute");
//		//
//		// int dayExpect = (hour * 60) + minute;
//		//
//		// String expect = date + "-" + (String.format("%04d", dayExpect));
//		//
//		// System.out.println(expect);
//	}
//
//	public static String convertCode2(int onlinenumber) {
//		String[] chars = (onlinenumber+"").split("");
//		int sum = 0;
//		for (String aChar : chars) {
//			if (aChar != null && !"".equals(aChar)) {
//				sum += (Integer.valueOf(aChar));
//			}
//		}
//
//		// 万位
//		String wan = sum + "";
//		wan = wan.substring(wan.length() - 1);
//
//		String qian = chars[chars.length - 4] + "";
//		String bai = chars[chars.length - 3] + "";
//		String shi = chars[chars.length - 2] + "";
//		String ge = chars[chars.length - 1] + "";
//
//		return wan + "," + qian + "," + bai + "," + shi + "," + ge;
//	}
//}