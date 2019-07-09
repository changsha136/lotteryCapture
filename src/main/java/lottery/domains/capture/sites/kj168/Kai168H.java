// package lottery.domains.capture.sites.kj168;
//
// import javautils.date.DateUtil;
// import lottery.domains.capture.utils.open.OpenTimeUtil;
// import lottery.domains.content.dao.LotteryCrawlerStatusDao;
// import lottery.domains.content.dao.LotteryOpenCodeDao;
// import lottery.domains.content.entity.LotteryCrawlerStatus;
// import lottery.domains.content.entity.LotteryOpenCode;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.beans.factory.annotation.Qualifier;
// import org.springframework.stereotype.Component;
//
// import java.util.List;
//
// /**
//  * 时时彩 11选5 快3
//  */
// @Component
// public class Kai168H {
//
// 	private static final Logger logger = LoggerFactory.getLogger(Kai168H.class);
//
// 	@Autowired
// 	private LotteryCrawlerStatusDao lotteryCrawlerStatusDao;
//
// 	@Autowired
// 	private LotteryOpenCodeDao lotteryOpenCodeDao;
//
// 	public boolean start(String lotteryName) {
// 		logger.debug("开始抓取" + lotteryName + "数据>>>>>>>>>>>>>>>>");
// 		LotteryCrawlerStatus lotteryCrawlerStatus = lotteryCrawlerStatusDao
// 				.get(lotteryName);
// 		String lastExpect = lotteryCrawlerStatus.getLastExpect();
//
// 		String date = DateUtil.formatTime(lastExpect.substring(0, 8),
// 				"yyyyMMdd", "yyyy-MM-dd");
// 		String expect = lastExpect.substring(9);
// 		// 如果是最后一期，那么就抓明天的
// 		if (lotteryCrawlerStatus.getTimes() == Integer.parseInt(expect)) {
// 			date = DateUtil.calcNextDay(date);
// 		}
// 		logger.debug("目标日期：" + date);
// 		List<LotteryOpenCode> list = getOpenCode(lotteryName, date);
// 		String currTime = DateUtil.getCurrentTime();
// 		for (LotteryOpenCode lotteryOpenCode : list) {
// 			// 设置数据来源
// 			lotteryOpenCode.setRemarks("kai168.com");
// 			String currExpect = lotteryOpenCode.getExpect();
// 			// 如果大于上一期，插入
// 			if (currExpect.compareTo(lastExpect) > 0) {
// 				boolean isSucc = lotteryOpenCodeDao.add(lotteryOpenCode);
// 				if (isSucc) {
// 					// 更新状态，最后成功抓到数据的期数、时间
// 					lotteryCrawlerStatusDao.update(lotteryName, currExpect,
// 							currTime);
// 				}
// 			}
// 		}
// 		// 如果不是当前期，那么就继续抓吧
// 		if (!isCurrExpect(lotteryName)) {
// 			logger.debug("不是最新的数据，10秒后继续抓...");
// 			return false;
// 		}
// 		return true;
// 	}
//
// 	/**
// 	 * 获取开奖数据
// 	 *
// 	 * @param lotteryName
// 	 * @param date
// 	 * @return
// 	 */
// 	private List<LotteryOpenCode> getOpenCode(String lotteryName, String date) {
// 		switch (lotteryName) {
// 		case "xjssc":
// 			return Kai168CrawlerUtil.getXjsscOpenCode(lotteryName, date);
// 		case "cqssc":
// 			return Kai168CrawlerUtil.getCqsscOpenCode(lotteryName, date);
// //		case "jxssc":
// //			return Kai168CrawlerUtil.getJxsscOpenCode(lotteryName, date);
// 		case "tjssc":
// 			return Kai168CrawlerUtil.getTjsscOpenCode(lotteryName, date);
// 		case "gd11x5":
// 			return Kai168CrawlerUtil.getGd11x5OpenCode(lotteryName, date);
// 		case "jx11x5":
// 			return Kai168CrawlerUtil.getJx11x5OpenCode(lotteryName, date);
// 		case "cq11x5":
// 			return Kai168CrawlerUtil.getCq11x5OpenCode(lotteryName, date);
// 		case "ah11x5":
// 			return Kai168CrawlerUtil.getAh11x5OpenCode(lotteryName, date);
// 		case "sh11x5":
// 			return Kai168CrawlerUtil.getSh11x5OpenCode(lotteryName, date);
// 		case "sd11x5":
// 			return Kai168CrawlerUtil.getSd11x5OpenCode(lotteryName, date);
// 		case "jsk3":
// 			return Kai168CrawlerUtil.getJsk3OpenCode(lotteryName, date);
// 		case "ahk3":
// 			return Kai168CrawlerUtil.getAhk3OpenCode(lotteryName, date);
// 		case "jlk3":
// 			return Kai168CrawlerUtil.getJlk3OpenCode(lotteryName, date);
// 		case "bjkl8":
// 			return Kai168CrawlerUtil.getBjkl8OpenCode(lotteryName, date);
// 		case "bjpk10":
// 			return Kai168CrawlerUtil.getBjpk10OpenCode(lotteryName, date);
//
// 		default:
// 			return null;
// 		}
// 	}
//
// 	@Autowired
// 	@Qualifier(value = "highOpenTimeUtil")
// 	private OpenTimeUtil highOpenTimeUtil;
//
// 	/**
// 	 * 判断是否是当前期
// 	 *
// 	 * @param lotteryName
// 	 * @return
// 	 */
// 	public boolean isCurrExpect(String lotteryName) {
// 		String currTime = DateUtil.getCurrentTime();
// 		String currExpect = highOpenTimeUtil.getCurrExpect(lotteryName,
// 				currTime);
// 		logger.debug("当前期时间：" + currTime + " 期号：" + currExpect);
// 		LotteryCrawlerStatus lotteryCrawlerStatus = lotteryCrawlerStatusDao
// 				.get(lotteryName);
// 		String lastExpect = lotteryCrawlerStatus.getLastExpect();
// 		logger.debug("当前期：" + currExpect + " 最后一期：" + lastExpect);
// 		return lastExpect.equals(currExpect);
// 	}
// }