package lottery.domains.capture.utils.open;

import java.util.List;

import lottery.domains.content.entity.LotteryOpenTime;
import lottery.domains.content.pool.LotteryDataFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 低频彩开奖信息
 * 福彩3D、排列三
 */
@Component
public class LowOpenTimeUtil implements OpenTimeUtil {
	
	@Autowired
	private LotteryDataFactory df;

	@Override
	public String getCurrExpect(String lotteryName, String currTime) {
		List<LotteryOpenTime> list = df.listLotteryOpenTime(lotteryName);
		for (int i = 0, j = list.size(); i < j; i++) {
			LotteryOpenTime tmpBean = list.get(i);
			String startTime = tmpBean.getStartTime();
			String stopTime = tmpBean.getStopTime();
			if(currTime.compareTo(startTime) >= 0 && currTime.compareTo(stopTime) < 0) {
				if(i == 0) {
					return null;
				}
				tmpBean = list.get(i - 1);
				return tmpBean.getExpect();
			}
		}
		return null;
	}

	@Override
	public String getNextExpect(String lotteryName, String lastExpect) {
		List<LotteryOpenTime> list = df.listLotteryOpenTime(lotteryName);
		for (int i = 0, j = list.size(); i < j; i++) {
			LotteryOpenTime tmpBean = list.get(i);
			String expect = tmpBean.getExpect();
			if(lastExpect.compareTo(expect) < 0) {
				return expect;
			}
		}
		return null;
	}

}