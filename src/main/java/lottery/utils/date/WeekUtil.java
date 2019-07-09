package lottery.utils.date;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class WeekUtil {
	/**
	 * 取得当前日期是多少周
	 * @param date
	 * @return
	 */
	public static int getWeekOfYear(Date date) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.setMinimalDaysInFirstWeek(7);
		c.setTime(date);
		return c.get(Calendar.WEEK_OF_YEAR);
	}

	/**
	 * 得到某一年周的总数
	 * @param year
	 * @return
	 */
	public static int getMaxWeekNumOfYear(int year) {
		Calendar c = new GregorianCalendar();
		c.set(year, Calendar.DECEMBER, 31, 23, 59, 59);
		return getWeekOfYear(c.getTime());
	}

	/**
	 * 得到某年某周的第一天
	 * @param year
	 * @param week
	 * @return
	 */
	public static Date getFirstDayOfWeek(int year, int week) {
		Calendar c = new GregorianCalendar();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, Calendar.JANUARY);
		c.set(Calendar.DATE, 1);

		Calendar cal = (GregorianCalendar) c.clone();
		cal.add(Calendar.DATE, week * 7);
		return getFirstDayOfWeek(cal.getTime(), Calendar.MONDAY);
	}
	
	/**
	 * 得到某年某周的最后一天
	 * @param year
	 * @param week
	 * @return
	 */
	public static Date getLastDayOfWeek(int year, int week) {
		Calendar c = new GregorianCalendar();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, Calendar.JANUARY);
		c.set(Calendar.DATE, 1);

		Calendar cal = (GregorianCalendar) c.clone();
		cal.add(Calendar.DATE, week * 7);

		return getLastDayOfWeek(cal.getTime(), Calendar.MONDAY);
	}

	/**
	 * 取得当前日期所在周的第一天
	 * @param date
	 * @return
	 */
	public static Date getFirstDayOfWeek(Date date, int firstDayOfWeek) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(firstDayOfWeek);
		c.setTime(date);
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek()); // Monday
		return c.getTime();
	}
	
	/**
	 * 取得当前日期所在周的最后一天
	 * @param date
	 * @return
	 */
	public static Date getLastDayOfWeek(Date date, int firstDayOfWeek) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(firstDayOfWeek);
		c.setTime(date);
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + 6); // Sunday
		return c.getTime();
	}
	
	/**
	 * 取得时间内的所对应周的第一天
	 * @param time
	 * @return
	 */
	public static String getFirstDayOfWeek(String time) {
		long date = DateUtil.formatTime(time, "yyyy-MM-dd");
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.SUNDAY);
		c.setTimeInMillis(date);
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek()); // Monday
		String firstDayOfWeek = DateUtil.formatTime(c.getTime(), "yyyy-MM-dd");
		return firstDayOfWeek;
	}
	
	/**
	 * 取得时间内的所对应周的最后一天
	 * @param time
	 * @return
	 */
	public static String getLastDayOfWeek(String time) {
		long date = DateUtil.formatTime(time, "yyyy-MM-dd");
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.SUNDAY);
		c.setTimeInMillis(date);
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + 6); // Sunday
		String lastDayOfWeek = DateUtil.formatTime(c.getTime(), "yyyy-MM-dd");
		return lastDayOfWeek;
	}
}
