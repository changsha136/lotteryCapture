package lottery.domains.capture.utils.open;

import javautils.date.DateUtil;

import java.util.Date;

public class OpenTimeTransUtil {

	/**
	 * <p>根据时间转换成对应期数，refDate和refExpect必须按规则配置</p>
	 * <p>韩国1.5分彩某天第1期在这里看：http://www.jlotto.kr/keno.aspx?method=kenoWinNoList</p>
	 * <p>北京快乐8某天第1期在这里看：http://www.bwlc.net/bulletin/prevkeno.html?dates=2016-09-16&page=3</p>
	 * <p>北京PK10某天第1期在这里看：http://www.bwlc.net/bulletin/prevtrax.html?dates=2016-09-16&page=2</p>
	 * <br/>
	 *
	 * <ul>
	 *     <li>韩国1.5分彩 2016-09-16第1期号1638731</li>
	 *     <li>北京快乐8 2016-09-16第1期号781437</li>
	 *     <li>北京PK10 2016-09-16第1期号576009</li>
	 * </ul>
	 * @param expect 当前开奖时间，配置在数据库的，未经过计算的
	 * @param refDate      引用日期，用来计算的日期，随便配置，引用日期必须要大于要转换的日期
	 * @param refExpect    引用期数，引用期数必须是引用日期当天的第1期期号
	 * @param times        每天多少期
	 * @return 返回计算后的对象
	 */
	public static String trans(String expect, String refDate, int refExpect, int times) {
		String calcExpect = expect;

		// 当前日期
		Date currentDate = DateUtil.stringToDate(calcExpect.substring(0, 8), "yyyyMMdd");
		// 当天第多少期
		int currentTimes = Integer.valueOf(calcExpect.substring(9));

		// 引用日期时间对象
		Date refDateDate = DateUtil.stringToDate(refDate, "yyyy-MM-dd");

		// 相差天数=当前日期-引用日期
		int disDays = DateUtil.calcDays(currentDate, refDateDate);

		// 相差期数 = 相差天数 * 每天期数 + 当天第多少期
		int disTimes = disDays * times + currentTimes;

		// 最终期数 = 引用期数+相差期数
		int finalExpect = refExpect + disTimes;

		return String.valueOf(finalExpect);
	}

	/**
	 * 转换期数，例如659897，转换后20141024-001
	 *
	 * @param realExpect 计算期数
	 * @param refDate    引用日期
	 * @param refExpect  引用期数
	 * @param times      每天多少期
	 * @return
	 */
	public static String trans(int realExpect, String refDate, int refExpect, int times) {
		// 相差期数 = 当前期数 - 引用期数
		int disTimes = realExpect - refExpect;

		// 相差天数 = 相差期数 / 每天期数
		int disDays = disTimes / times;

		// 剩余期数 = 相差期数 % 每天期数
		int remainTimes = disTimes % times;

		// 计算后的日期
		String currentDate = DateUtil.calcNewDay(refDate, disDays);

		return currentDate.replace("-", "") + "-" + String.format("%03d", remainTimes);
	}
}