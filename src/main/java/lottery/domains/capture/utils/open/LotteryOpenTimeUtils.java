package lottery.domains.capture.utils.open;

import java.util.List;

import javautils.date.DateUtil;
import javautils.date.Moment;
import lottery.domains.content.entity.LotteryOpenTime;
import lottery.domains.content.pool.LotteryDataFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LotteryOpenTimeUtils {

	@Autowired
	private LotteryDataFactory df;

	public String getCurrExpect(String lotteryName) {
		Moment m = new Moment();
		String curTime = m.toCurTime();
		curTime = "00:00:00";
		String toDay = m.toSimpleDate();
		List<LotteryOpenTime> list = df.listLotteryOpenTime(lotteryName);
		for (int i = 0; i < list.size(); i++) {
			if (i < list.size() - 1) {
				LotteryOpenTime ot1 = list.get(i);
				LotteryOpenTime ot2 = list.get(i + 1);
				if (curTime.compareTo(ot1.getOpenTime()) >= 0
						&& curTime.compareTo(ot2.getOpenTime()) < 0) {
					String expect = toDay.replace("-", "") + "-"
							+ ot1.getExpect();
					return expect;
				} else if (curTime.compareTo(ot1.getOpenTime()) >= 0
						&& ot1.getOpenTime().compareTo(ot2.getOpenTime()) > 0) {
					String yesterDay = m.subtract(1, "d").toSimpleDate();
					String expect = yesterDay.replace("-", "") + "-"
							+ ot1.getExpect();
					return expect;
				}
			} else {
				LotteryOpenTime lotteryOpenTime = list.get(list.size() - 1);
				if (lotteryOpenTime.getOpenTime().compareTo(
						lotteryOpenTime.getStartTime()) < 0) {
					String yesterDay = m.subtract(1, "d").toSimpleDate();
					String expect = yesterDay.replace("-", "") + "-"
							+ lotteryOpenTime.getExpect();
					return expect;
				} else {
					String expect = toDay.replace("-", "") + "-"
							+ lotteryOpenTime.getExpect();
					return expect;
				}

			}
		}
		return "";
	}
}
