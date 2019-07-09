package lottery.domains.capture.utils.open;

import java.util.List;

import javautils.date.DateUtil;
import lottery.domains.content.dao.LotteryCrawlerStatusDao;
import lottery.domains.content.entity.LotteryCrawlerStatus;
import lottery.domains.content.entity.LotteryOpenTime;
import lottery.domains.content.pool.LotteryDataFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 高频彩开奖信息
 * 时时彩、十一选五、快3、北京快乐8、北京PK拾
 */
@Component
public class HighOpenTimeUtil implements OpenTimeUtil {
	
	@Autowired
	private LotteryDataFactory df;
	
	@Autowired
	private LotteryCrawlerStatusDao lotteryCrawlerStatusDao;
	
	public String getExpect(String lotteryName, String currTime) {
		List<LotteryOpenTime> list = df.listLotteryOpenTime(lotteryName);
		String currDate = currTime.substring(0, 10);
		String nextDate = DateUtil.calcNextDay(currDate);
		String lastDate = DateUtil.calcLastDay(currDate);
		for (int i = 0, j = list.size(); i < j; i++) {
			LotteryOpenTime tmpBean = list.get(i);
			String startDate = currDate; // 开始日期
			String stopDate = currDate; // 结束日期
			String openDate = currDate; // 开奖日期
			String expectDate = currDate; // 期号日期
			String startTime = tmpBean.getStartTime(); // 开始时间
			String stopTime = tmpBean.getStopTime(); // 结束时间
			String openTime = tmpBean.getOpenTime(); // 开奖时间
			String expect = tmpBean.getExpect();
			if(i == 0) {
				if(startTime.compareTo(stopTime) > 0) {
					startDate = lastDate;
				}
			} else if(i == j - 1) {
				if(startTime.compareTo(stopTime) > 0) {
					stopDate = nextDate;
				}
				if(startTime.compareTo(openTime) > 0) {
					openDate = nextDate;
				}
				if(currTime.compareTo(stopDate + " " + stopTime) >= 0) {
					tmpBean = list.get(0);
					startDate = nextDate; // 开始日期
					stopDate = nextDate; // 结束日期
					openDate = nextDate; // 开奖日期
					expectDate = nextDate; // 期号日期
					startTime = tmpBean.getStartTime(); // 开始时间
					stopTime = tmpBean.getStopTime(); // 结束时间
					openTime = tmpBean.getOpenTime(); // 开奖时间
					expect = tmpBean.getExpect();
					if(startTime.compareTo(stopTime) > 0) {
						startDate = currDate;
					}
				}
			} else {
				if(startTime.compareTo(stopTime) > 0) {
					stopDate = nextDate;
				}
				if(startTime.compareTo(openTime) > 0) {
					openDate = nextDate;
				}
			}
			if(!tmpBean.getIsTodayExpect()) {
				expectDate = lastDate;
			}
			startTime = startDate + " " + startTime;
			stopTime = stopDate + " " + stopTime;
			openTime = openDate + " " + openTime;
			expect = expectDate.replace("-", "") + "-" + expect;
			if(currTime.compareTo(startTime) >= 0 && currTime.compareTo(stopTime) < 0) {
				return expect;
			}
		}
		return null;
	}

	@Override
	public String getCurrExpect(String lotteryName, String currTime) {
		LotteryCrawlerStatus lotteryCrawlerStatus = lotteryCrawlerStatusDao.get(lotteryName);
		// 先得到当前销售期
		String tmpExpect = getExpect(lotteryName, currTime);
		String tmpDate = tmpExpect.substring(0, 8);
		String currDate = DateUtil.formatTime(tmpDate, "yyyyMMdd", "yyyy-MM-dd");
		String currExpect = tmpExpect.substring(9);
		String lastDate = currDate;
		int lastExpect = Integer.parseInt(currExpect);
		int times = lotteryCrawlerStatus.getTimes();
		// 如果是第1期，那么上一期的就是昨天最后一期
		if(lastExpect == 1) {
			lastDate = DateUtil.calcLastDay(currDate);
			lastExpect = times;
		} else {
			lastExpect--;
		}

		int formatCount = 3;
		if ("fgffc".equals(lotteryName)) {
			formatCount = 4;
		}

		String expect = lastDate.replaceAll("-", "") + "-" + String.format("%0"+formatCount+"d", lastExpect);
		return expect;
	}

	@Override
	public String getNextExpect(String lotteryName, String lastExpect) {
		return null;
	}

}