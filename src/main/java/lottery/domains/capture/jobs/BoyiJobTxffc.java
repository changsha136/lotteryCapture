//package lottery.domains.capture.jobs;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//
//import javautils.date.Moment;
//import javautils.http.HttpClientUtil;
//import lottery.domains.capture.sites.b1cp.BoyiBean;
//import lottery.domains.capture.utils.CodeValidate;
//import lottery.domains.capture.utils.ExpectValidate;
//import lottery.domains.content.biz.LotteryOpenCodeService;
//import lottery.domains.content.entity.LotteryOpenCode;
//import org.apache.commons.collections.CollectionUtils;
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
// * 博易，主要用来获取腾讯分分彩数据，但当前期不获取，当前期以PCQQJob为准
// * 在http://www.jiangyuan365.com注册账号后获取token
// */
//@Component
//public class BoyiJobTxffc {
//	private static final Logger logger = LoggerFactory.getLogger(BoyiJobTxffc.class);
//	private static boolean isRuning = false;
//	private static final String TOKEN = "93022be9ac9cfc75"; // token，注册后有 setyourtokenhere替换为自己的token 在
//	private static final String URL = "http://api.b1cp.com/api?p=json&t=txffc&limit=10&token=" + TOKEN;
//
//	// 彩票code列表
//	private static final Map<String, String> LOTTERIES = new HashMap<>();
//	static {
//		// key：对方code，value：我方code
//		LOTTERIES.put("txffc", "txffc"); // 腾讯分分彩
//	}
//
//	@Autowired
//	private LotteryOpenCodeService lotteryOpenCodeService;
//
//
////    @Scheduled(cron = "0/20 * * * * *") // 注意频率，每次间隔大于1秒
//	@Scheduled(cron = "6,10,15,20,25,30,35 * * * * *")
//	// @PostConstruct
//	public void execute() {
//		synchronized (BoyiJobTxffc.class) {
//			if (isRuning == true) {
//				return;
//			}
//			isRuning = true;
//		}
//
//		try {
//			logger.debug("开始抓取博易腾讯分分彩开奖数据>>>>>>>>>>>>>>>>");
//
//			long start = System.currentTimeMillis();
//			start();
//			long spend = System.currentTimeMillis() - start;
//
//			logger.debug("完成抓取博易腾讯分分彩开奖数据>>>>>>>>>>>>>>>>耗时{}", spend);
//		} catch (Exception e) {
//			logger.error("抓取博易腾讯分分彩开奖数据出错", e);
//		} finally {
//			isRuning = false;
//		}
//	}
//
//	private void start() {
//		for (String lottery : LOTTERIES.keySet()) {
//			try {
//				String realName = LOTTERIES.get(lottery);
//
//				String result = getResult(lottery, 10);
//
//				handleData(realName, result);
//			} catch (Exception e) {
//				logger.error("抓取博易腾讯分分彩"+lottery+"开奖数据出错", e);
//			}
//		}
//	}
//
//	public String getResult(String name, int num) {
//		String result = get(URL);
//		return result;
//	}
//
//	private void handleData(String realName, String result) {
//		if (StringUtils.isEmpty(result)) {
//			return;
//		}
//
//		JSONObject jsonObject = JSON.parseObject(result);
//		JSONArray jsonArr = jsonObject.getJSONArray("data");
//		List<BoyiBean> openCodes = JSON.parseArray(jsonArr.toJSONString(), BoyiBean.class);
//		if (CollectionUtils.isEmpty(openCodes)) {
//			return;
//		}
//
//		// 处理数据
//		for (BoyiBean openCode : openCodes) {
//			handleBean(realName, openCode);
//		}
//	}
//
//	private boolean handleBean(String realName, BoyiBean openCode) {
//		openCode.setExpect(formartExpect(openCode.getExpect()));
//		LotteryOpenCode dbData = lotteryOpenCodeService.get(realName, openCode.getExpect());
//		if (dbData != null) {
//			if (!dbData.getCode().equals(openCode.getOpencode())) {
//				logger.error("博易抓取时遇到错误：抓取{}期开奖号码{}与数据库已有开奖号码{}不符", openCode.getExpect(), openCode.getOpencode(), dbData.getCode());
//				return false;
//			}
//			return true;
//		}
//		
//		// 如果本期和上奖开奖号码相同，那么把开奖号码状态改为无效撤单
//		LotteryOpenCode lotteryOpenCode = new LotteryOpenCode();
//		lotteryOpenCode.setInterfaceTime(openCode.getOpentime());
//		lotteryOpenCode.setLottery(realName);
//		lotteryOpenCode.setTime(new Moment().toSimpleTime());
//		lotteryOpenCode.setOpenStatus(0);
//		lotteryOpenCode.setRemarks("b1cp.com");
//
//		switch (realName) {
//			// 腾讯分分彩
//			case "txffc":
//				lotteryOpenCode.setCode(openCode.getOpencode());
//				lotteryOpenCode.setExpect(openCode.getExpect());
//				break;
//			default:
//				break;
//		}
//
//		if (StringUtils.isEmpty(lotteryOpenCode.getCode()) || StringUtils.isEmpty(lotteryOpenCode.getExpect())) {
//			return false;
//		}
//
//		if (CodeValidate.validate(realName, lotteryOpenCode.getCode()) == false) {
//			logger.error("博易腾讯分分彩" + realName + "抓取号码" + lotteryOpenCode.getCode() + "错误");
//			return false;
//		}
//
//		if (ExpectValidate.validate(realName, lotteryOpenCode.getExpect()) == false) {
//			logger.error("博易腾讯分分彩" + realName + "抓取期数" + lotteryOpenCode.getExpect() + "错误");
//			return false;
//		}
//
//		boolean added = lotteryOpenCodeService.add(lotteryOpenCode, true);
//		if (added) {
//			logger.info("博易官网成功抓取腾讯分分彩{}期开奖号码{}", openCode.getExpect(), openCode.getOpencode());
//			// 腾讯龙虎斗
//			if ("txffc".equals(realName)) {
//				LotteryOpenCode txlhdCode = new LotteryOpenCode("txlhd", lotteryOpenCode.getExpect(), lotteryOpenCode.getCode(), lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(), null, lotteryOpenCode.getRemarks());
//				txlhdCode.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
//				lotteryOpenCodeService.add(txlhdCode, false);
//			}
//		}
//
//		return added;
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
//	private String formartExpect(String expect){
//		String date = expect.substring(0,8);
//		String num = String.format("%04d", Integer.valueOf(expect.substring(8)));
//		return date + "-" +num;
//	}
//	
//	public static String get(String urlAll) {
//		try {
//			String result = HttpClientUtil.get(urlAll, null, 5000);
//			return result;
//		} catch (Exception e) {
//			logger.error("请求博易腾讯分分彩出错", e);
//			return null;
//		}
//	}
//}