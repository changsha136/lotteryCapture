package lottery.domains.capture.utils.open;

import javautils.date.Moment;
import lottery.domains.content.entity.Lottery;
import lottery.domains.content.pool.LotteryDataFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LotteryOpenUtil {
	
	@Autowired
	private LotteryDataFactory dataFactory;

	/**
	 * 物理计算指定期号的上一期开奖号码
	 * @param lotteryShortName 彩票
	 * @param expect 期号
	 * @return
	 */
	public String subtractExpect(String lotteryShortName, String expect) {
		Lottery lottery = dataFactory.getLottery(lotteryShortName);
		if (lottery == null) return null;

		String subExpect;
		if (expect.indexOf("-") <= -1) {
			Integer integerExpect = Integer.valueOf(expect);
			integerExpect -= 1;

			if (integerExpect.toString().length() >= expect.length()) {
				subExpect = integerExpect.toString();
			}
			else {
				subExpect = String.format("%0"+expect.length()+"d", integerExpect);
			}
		}
		else {
			String[] split = expect.split("-");
			int formatCount = split[1].length();
			String date = split[0];
			String currExpect = split[1];

			if (currExpect.equals("001") || currExpect.equals("0001")) {
				date = new Moment().fromDate(date).subtract(1, "days").format("yyyyMMdd");
				subExpect = String.format("%0"+formatCount+"d", lottery.getTimes());
			}
			else {
				Integer integer = Integer.valueOf(currExpect);
				integer -= 1;
				if (integer.toString().length() >= formatCount) {
					subExpect = integer.toString();
				}
				else {
					subExpect = String.format("%0"+formatCount+"d", integer);
				}

			}
			subExpect = date + "-" + subExpect;
		}

		return subExpect;
	}
}