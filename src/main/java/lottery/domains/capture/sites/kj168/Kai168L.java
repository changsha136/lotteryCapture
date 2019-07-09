// package lottery.domains.capture.sites.kj168;
//
// import javautils.date.DateUtil;
// import lottery.domains.capture.utils.open.OpenTimeUtil;
// import lottery.domains.content.dao.LotteryCrawlerStatusDao;
// import lottery.domains.content.dao.LotteryOpenCodeDao;
// import lottery.domains.content.entity.LotteryCrawlerStatus;
// import lottery.domains.content.entity.LotteryOpenCode;
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
//  * 福彩3d
//  * 排列3
//  */
// @Component
// public class Kai168L {
//
// 	private static final Logger logger = LoggerFactory.getLogger(Kai168L.class);
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
// 		String expect = getNextExpect(lotteryName, lastExpect);
// 		logger.debug("目标期数：" + expect);
// 		List<LotteryOpenCode> list = getOpenCode(lotteryName);
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
// 		if(!isCurrExpect(lotteryName)) {
// 			logger.debug("不是最新的数据，1分钟后继续抓...");
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
// 	private List<LotteryOpenCode> getOpenCode(String lotteryName) {
// 		switch (lotteryName) {
// 		case "fc3d":
// 			return Kai168CrawlerUtil.getFc3dOpenCode(lotteryName);
// 		case "pl3":
// 			return Kai168CrawlerUtil.getPl3OpenCode(lotteryName);
// 		default:
// 			return null;
// 		}
// 	}
//
// 	@Autowired
// 	@Qualifier(value = "lowOpenTimeUtil")
// 	private OpenTimeUtil lowOpenTimeUtil;
//
// 	@Autowired
// 	private LotteryDataFactory df;
//
// 	/**
// 	 * 判断是否是当前期
// 	 * @param lotteryName
// 	 * @return
// 	 */
// 	public boolean isCurrExpect(String lotteryName) {
// 		String currTime = DateUtil.getCurrentTime();
// 		String currExpect = lowOpenTimeUtil.getCurrExpect(lotteryName, currTime);
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
// 	public String getNextExpect(String lotteryName, String lastExpect) {
// 		return lowOpenTimeUtil.getNextExpect(lotteryName, lastExpect);
// 	}
// }