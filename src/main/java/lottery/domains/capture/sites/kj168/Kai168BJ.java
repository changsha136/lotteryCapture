// package lottery.domains.capture.sites.kj168;
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
// /**
//  * 北京快乐8
//  * 北京pk10
//  */
// @Component
// public class Kai168BJ {
//
// 	private final static Logger logger = LoggerFactory.getLogger(Kai168BJ.class);
//
// 	@Autowired
// 	private LotteryCrawlerStatusDao lotteryCrawlerStatusDao;
//
// 	@Autowired
// 	private LotteryOpenCodeDao lotteryOpenCodeDao;
//
// 	public boolean start(String lotteryName) {
// 		logger.debug("开始抓取" + lotteryName  + "数据>>>>>>>>>>>>>>>>");
// 		LotteryCrawlerStatus lotteryCrawlerStatus = lotteryCrawlerStatusDao.get(lotteryName);
// 		String lastExpect = lotteryCrawlerStatus.getLastExpect();
// 		String expect = getNextExpect(lotteryName, lastExpect, lotteryCrawlerStatus.getTimes());
// 		String date = DateUtil.formatTime(expect.substring(0, 8), "yyyyMMdd", "yyyy-MM-dd");
// 		logger.debug("目标期数：" + expect);
// 		List<LotteryOpenCode> list = getOpenCode(lotteryName, date);
// 		String currTime = DateUtil.getCurrentTime();
// 		for (LotteryOpenCode lotteryOpenCode : list) {
// 			// 设置数据来源
// 			lotteryOpenCode.setRemarks("kai168.com");
// 			String currExpect = lotteryOpenCode.getExpect();
// 			// 如果大于上一期，插入
// 			if(currExpect.compareTo(lastExpect) > 0) {
// 				boolean isSucc = lotteryOpenCodeDao.add(lotteryOpenCode);
// 				if(isSucc) {
// 					// 更新状态，最后成功抓到数据的期数、时间
// 					lotteryCrawlerStatusDao.update(lotteryName, currExpect, currTime);
// 				}
// 			}
// 		}
// 		// 如果不是当前期，那么就继续抓吧
// 		if(!isCurrExpect(lotteryName, lotteryCrawlerStatus.getTimes())) {
// 			logger.debug("不是最新的数据，10秒后继续抓...");
// 			return false;
// 		}
// 		return true;
// 	}
//
// 	/**
// 	 * 获取开奖数据
// 	 * @param lotteryName
// 	 * @param date
// 	 * @return
// 	 */
// 	private List<LotteryOpenCode> getOpenCode(String lotteryName, String date) {
// 		switch (lotteryName) {
// 		case "bjkl8":
// 			return Kai168CrawlerUtil.getBjkl8OpenCode(lotteryName, date);
// 		case "bjpk10":
// 			return Kai168CrawlerUtil.getBjpk10OpenCode(lotteryName, date);
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
// 		String currExpect = OpenTimeTransUtil.trans(expect, refDate, refExpect, times);
// 		LotteryCrawlerStatus lotteryCrawlerStatus = lotteryCrawlerStatusDao.get(lotteryName);
// 		String lastExpect = lotteryCrawlerStatus.getLastExpect();
// 		logger.debug("当前期：" + currExpect + " 最后一期：" + lastExpect);
// 		return lastExpect.equals(currExpect);
// 	}
//
// 	/**
// 	 * 获取下一期
// 	 * @param lastExpect
// 	 * @return
// 	 */
// 	public String getNextExpect(String lotteryName, String lastExpect, int times) {
// 		int nextExpect = Integer.parseInt(lastExpect) + 1;
// 		logger.debug("获取下一期：" + nextExpect);
// 		String refLotteryName = lotteryName + "_ref"; // 获取坐标名称
// 		List<LotteryOpenTime> opList = df.listLotteryOpenTime(refLotteryName);
// 		LotteryOpenTime lotteryOpenTime = opList.get(0);
// 		String refDate = lotteryOpenTime.getOpenTime();
// 		int refExpect = Integer.parseInt(lotteryOpenTime.getExpect());
// 		return OpenTimeTransUtil.trans(nextExpect, refDate, refExpect, times);
// 	}
// }