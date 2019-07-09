// package lottery.domains.capture.sites.bwlc;
//
// import javautils.date.DateUtil;
// import lottery.domains.capture.utils.open.OpenTimeTransUtil;
// import lottery.domains.capture.utils.open.OpenTimeUtil;
// import lottery.domains.content.dao.LotteryCrawlerStatusDao;
// import lottery.domains.content.dao.LotteryOpenCodeDao;
// import lottery.domains.content.entity.LotteryCrawlerStatus;
// import lottery.domains.content.entity.LotteryOpenCode;
// import lottery.domains.content.entity.LotteryOpenTime;
// import lottery.domains.content.pool.LotteryDataFactory;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.beans.factory.annotation.Qualifier;
// import org.springframework.stereotype.Component;
//
// import java.util.List;
//
// @Component
// public class BwlcCrawler {
//
// 	private static final Logger logger = LoggerFactory.getLogger(BwlcCrawler.class);
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
// 		String expect = getNextExpect(lastExpect);
// 		logger.debug("目标期数：" + expect);
// 		List<LotteryOpenCode> list = getOpenCode(lotteryName, expect);
// 		String currTime = DateUtil.getCurrentTime();
// 		for (LotteryOpenCode lotteryOpenCode : list) {
// 			// 设置数据来源
// 			lotteryOpenCode.setRemarks("www.bwlc.net");
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
// 		if (!isCurrExpect(lotteryName, lotteryCrawlerStatus.getTimes())) {
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
// 	private List<LotteryOpenCode> getOpenCode(String lotteryName, String expect) {
// 		switch (lotteryName) {
// 		case "bjkl8":
// 			return BwlcCrawlerUtil.getBjkl8OpenCode(lotteryName, expect);
// 		case "bjpk10":
// 			return BwlcCrawlerUtil.getBjpk10OpenCode(lotteryName, expect);
// 		case "fc3d":
// 			return BwlcCrawlerUtil.getFc3dOpenCode(lotteryName, expect);
// 		default:
// 			return null;
// 		}
// 	}
//
// 	@Autowired
// 	@Qualifier(value = "highOpenTimeUtil")
// 	private OpenTimeUtil highOpenTimeUtil;
//
// 	@Autowired
// 	private LotteryDataFactory df;
//
// 	/**
// 	 * 判断是否是当前期
// 	 *
// 	 * @param lotteryName
// 	 * @return
// 	 */
// 	public boolean isCurrExpect(String lotteryName, int times) {
// 		String currTime = DateUtil.getCurrentTime();
// 		String expect = highOpenTimeUtil.getCurrExpect(lotteryName, currTime);
// 		String refLotteryName = lotteryName + "_ref"; // 获取坐标名称
// 		List<LotteryOpenTime> opList = df.listLotteryOpenTime(refLotteryName);
// 		LotteryOpenTime lotteryOpenTime = opList.get(0);
// 		String refDate = lotteryOpenTime.getOpenTime();
// 		int refExpect = Integer.parseInt(lotteryOpenTime.getExpect());
// 		String currExpect = OpenTimeTransUtil.trans(expect, refDate, refExpect,
// 				times);
// 		LotteryCrawlerStatus lotteryCrawlerStatus = lotteryCrawlerStatusDao
// 				.get(lotteryName);
// 		String lastExpect = lotteryCrawlerStatus.getLastExpect();
// 		logger.debug("当前期：" + currExpect + " 最后一期：" + lastExpect);
// 		return lastExpect.equals(currExpect);
// 	}
//
// 	/**
// 	 * 获取下一期
// 	 *
// 	 * @param lastExpect
// 	 * @return
// 	 */
// 	public String getNextExpect(String lastExpect) {
// 		return String.valueOf(Integer.parseInt(lastExpect) + 1);
// 	}
// }