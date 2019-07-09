// package lottery.domains.capture.sites.tjflcpw;
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
// @Component
// public class TjflcpwCrawler {
//
// 	private static final Logger logger = LoggerFactory.getLogger(TjflcpwCrawler.class);
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
// 		String nextExpect = getNextExpect(lastExpect, lotteryCrawlerStatus.getTimes());
// 		List<LotteryOpenCode> list = TjflcpwCrawlerUtil.getTjsscOpenCode(lotteryName, nextExpect);
// 		String currTime = DateUtil.getCurrentTime();
// 		for (LotteryOpenCode lotteryOpenCode : list) {
// 			// 设置数据来源
// 			lotteryOpenCode.setRemarks("www.tjflcpw.com");
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
// 			logger.debug("不是最新的数据，10秒后继续抓...");
// 			return false;
// 		}
// 		return true;
// 	}
//
// 	@Autowired
// 	@Qualifier(value = "highOpenTimeUtil")
// 	private OpenTimeUtil highOpenTimeUtil;
//
// 	/**
// 	 * 判断是否是当前期
// 	 * @param lotteryName
// 	 * @return
// 	 */
// 	public boolean isCurrExpect(String lotteryName) {
// 		String currTime = DateUtil.getCurrentTime();
// 		String currExpect = highOpenTimeUtil.getCurrExpect(lotteryName, currTime);
// 		LotteryCrawlerStatus lotteryCrawlerStatus = lotteryCrawlerStatusDao.get(lotteryName);
// 		String lastExpect = lotteryCrawlerStatus.getLastExpect();
// 		logger.debug("当前期：" + currExpect + " 最后一期：" + lastExpect);
// 		return lastExpect.equals(currExpect);
// 	}
//
// 	/**
// 	 * 获取下一期期号
// 	 * @param expect
// 	 * @param times
// 	 * @return
// 	 */
// 	public String getNextExpect(String expect, int times) {
// 		String date = expect.split("-")[0];
// 		int num = Integer.parseInt(expect.split("-")[1]);
// 		if(num == times) {
// 			num = 1;
// 			String d = DateUtil.formatTime(date, "yyyyMMdd", "yyyy-MM-dd");
// 			date = DateUtil.calcNextDay(d);
// 			date = DateUtil.formatTime(date, "yyyy-MM-dd", "yyyyMMdd");
// 		} else {
// 			num++;
// 		}
// 		return date + "-" + String.format("%03d", num);
// 	}
//
// }