package javautils.date;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateUtil {
	
	/**
	 * 获取当前时间
	 * @return
	 */
	public static String getCurrentTime() {
		GregorianCalendar g = new GregorianCalendar();
		return dateToString(g);
	}
	
	/**
	 * 获取当前日期
	 * @return
	 */
	public static String getCurrentDate() {
		GregorianCalendar g = new GregorianCalendar();
		return dateToStringSim(g);
	}

	/**
	 * 获取昨天日期
	 * @return
	 */
	public static String getYesterday() {
		GregorianCalendar g = new GregorianCalendar();
		g.set(Calendar.DATE, g.get(Calendar.DATE) - 1);
		return dateToStringSim(g);
	}
	
	/**
	 * 获取明天日期
	 * @return
	 */
	public static String getTomorrow() {
		GregorianCalendar g = new GregorianCalendar();
		g.set(Calendar.DATE, g.get(Calendar.DATE) + 1);
		return dateToStringSim(g);
	}
	
	/**
	 * 转换成时间
	 * @param ms
	 * @return
	 */
	public static String getTime(long ms) {
		GregorianCalendar g = new GregorianCalendar();
		g.setTimeInMillis(ms);
		return dateToString(g);
	}
	
	/**
	 * 根据时间获取GregorianCalendar
	 * @param time
	 * @return
	 */
	private static GregorianCalendar getCalendarByTime(String time, String format) {
		GregorianCalendar g = new GregorianCalendar();
		try {
			g.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time));
		} catch (ParseException e) {
			g = null;
		}
		return g;
	}
	
	/**
	 * 计算时间
	 * @param g
	 * @param seconds
	 * @return
	 */
	private static String calcDate(GregorianCalendar g, int seconds) {
		if(g == null) {
			g = new GregorianCalendar();
		}
		g.add(Calendar.SECOND, seconds);
		return dateToString(g);
	}
	
	/**
	 * 日期转字符串
	 * @param date
	 * @return
	 */
	public static String dateToString(Date date) {
		try {
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			String s = formatter.format(date);
			return s;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 字符串转日期
	 * @param date
	 * @return
	 */
	public static Date stringToDate(String date) {
		try {
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date d = (Date) formatter.parse(date);
			return d;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 字符串转日期, 自定义格式
	 * @param date
	 * @param format
	 * @return
	 */
	public static Date stringToDate(String date, String format) {
		try {
			DateFormat formatter = new SimpleDateFormat(format);
			Date d = (Date) formatter.parse(date);
			return d;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 获取日期相差毫秒数
	 * @param subDate
	 * @param minDate
	 * @return
	 */
	public static long calcDate(String subDate, String minDate) {
		long lSubDate = getCalendarByTime(subDate, "yyyy-MM-dd HH:mm:ss").getTimeInMillis();
		long lMinDate = getCalendarByTime(minDate, "yyyy-MM-dd HH:mm:ss").getTimeInMillis();
		return (lSubDate - lMinDate);
	}

	/**
	 * 根据日期计算相差天数，忽略时分秒
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static int calcDays(Date date1, Date date2) {
		Calendar date1Calendar = Calendar.getInstance();
		date1Calendar.setTime(date1);

		Calendar date2Calendar = Calendar.getInstance();
		date2Calendar.setTime(date2);

		int day1 = date1Calendar.get(Calendar.DAY_OF_YEAR);
		int day2 = date2Calendar.get(Calendar.DAY_OF_YEAR);

		return day1 - day2;
	}

	/**
	 * 根据差值计算出新的时间
	 * @param time
	 * @param seconds
	 * @return
	 */
	public static String calcDateByTime(String time, int seconds) {
		GregorianCalendar g = getCalendarByTime(time, "yyyy-MM-dd HH:mm:ss");
		return calcDate(g, seconds);
	}
	
	/**
	 * 根据天数计算出新的日期
	 * @param date
	 * @param days
	 * @return
	 */
	public static String calcNewDay(String date, int days) {
		GregorianCalendar g = new GregorianCalendar();
		g.setTimeInMillis(formatTime(date, "yyyy-MM-dd"));
		g.set(Calendar.DATE, g.get(Calendar.DATE) + days);
		return dateToStringSim(g);
	}
	
	/**
	 * 下一天
	 * @param date
	 * @return
	 */
	public static String calcNextDay(String date) {
		GregorianCalendar g = new GregorianCalendar();
		g.setTimeInMillis(formatTime(date, "yyyy-MM-dd"));
		g.set(Calendar.DATE, g.get(Calendar.DATE) + 1);
		return dateToStringSim(g);
	}
	
	/**
	 * 上一天
	 * @param date
	 * @return
	 */
	public static String calcLastDay(String date) {
		GregorianCalendar g = new GregorianCalendar();
		g.setTimeInMillis(formatTime(date, "yyyy-MM-dd"));
		g.set(Calendar.DATE, g.get(Calendar.DATE) - 1);
		return dateToStringSim(g);
	}
	
	private static Calendar getDateOfMonth(Calendar date, int num, boolean flag) {
		Calendar lastDate = (Calendar) date.clone();
		if(flag) {
			lastDate.add(Calendar.MONTH, num);
		}else {
			lastDate.add(Calendar.MONTH, -num);
		}
		
		return lastDate;
	}

	private static Calendar getDateOfLastMonth(String dateStr, int num, boolean flag) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date date = sdf.parse(dateStr);
			Calendar c = Calendar.getInstance();
			c.setTime(date);
			return getDateOfMonth(c, num, flag);
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"Invalid date format(yyyyMMdd): " + dateStr);
		}
	}
	
	/**
	 * 获取上个月的同一天
	 * @param date
	 * @param num
	 * @param flag
	 * @return
	 */
	public static String getSameDateOfLastMonth(String date, int num, boolean flag) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String lastDate = sdf.format(getDateOfLastMonth(date, num, flag).getTime());
		return lastDate;
	}
	
	/**
	 * GregorianCalendar转成字符串
	 * @param g
	 * @return
	 */
	private static String dateToString(GregorianCalendar g) {
		String year = String.valueOf(g.get(Calendar.YEAR));
		String month = String.format("%02d", g.get(Calendar.MONTH) + 1);
		String day =String.format("%02d", g.get(Calendar.DAY_OF_MONTH));
		String hours = String.format("%02d", g.get(Calendar.HOUR_OF_DAY));
		String minutes = String.format("%02d", g.get(Calendar.MINUTE));
		String seconds = String.format("%02d", g.get(Calendar.SECOND));
		return year + "-" + month + "-" + day + " " + hours + ":" + minutes + ":" + seconds;
	}
	
	/**
	 * 根据时间转成MS
	 * @param time
	 * @param format
	 * @return
	 */
	public static long stringToLong(String time, String format) {
		return stringToDate(time, format).getTime();
	}
	
	/**
	 * GregorianCalendar转成日期格式
	 * @param g
	 * @return
	 */
	public static String dateToStringSim(GregorianCalendar g) {
		String year = String.valueOf(g.get(Calendar.YEAR));
		String month = String.format("%02d", g.get(Calendar.MONTH) + 1);
		String day = String.format("%02d", g.get(Calendar.DAY_OF_MONTH));
		return year + "-" + month + "-" + day;
	}
	
	/**
	 * 获取当前时间
	 * @return
	 */
	public static String getCurTimeStr() {
		GregorianCalendar g = new GregorianCalendar();
		String year = String.valueOf(g.get(Calendar.YEAR));
		String month = String.format("%02d", g.get(Calendar.MONTH) + 1);
		String day = String.format("%02d", g.get(Calendar.DAY_OF_MONTH));
		String hours = String.format("%02d", g.get(Calendar.HOUR_OF_DAY));
		String minutes = String.format("%02d", g.get(Calendar.MINUTE));
		String seconds = String.format("%02d", g.get(Calendar.SECOND));
		
		return year + month + day + hours + minutes + seconds;
	}
	
	/**
	 * 格式化时间
	 * @param time
	 * @param oldFormat
	 * @param newFormat
	 * @return
	 */
	public static String formatTime(String time, String oldFormat,
			String newFormat) {
		return new SimpleDateFormat(newFormat).format(stringToDate(time,
				oldFormat));
	}
	
	/**
	 * 格式化日期
	 * @param date
	 * @param format
	 * @return
	 */
	public static String formatTime(Date date, String format) {
		return new SimpleDateFormat(format).format(date);
	}

	/**
	 * 格式化时间
	 * @param time
	 * @param format
	 * @return
	 */
	public static String formatTime(long time, String format) {
		return formatTime(getTime(time), "yyyy-MM-dd HH:mm:ss", format);
	}

	/**
	 * 获取时间, 根据格式化参数
	 * @param time
	 * @param format
	 * @return
	 */
	public static long formatTime(String time, String format) {
		return stringToDate(time, format).getTime();
	}
	
	public static String dateForm(String date, String config) {
		if ("MM/dd/yyyy".equals(config)) {
			String[] dateStrs = date.split("/");
			return dateStrs[2] + "-" + dateStrs[0] + "-" + dateStrs[1];
		}
		if ("MM-dd-yyyy".equals(config)) {
			String[] dateStrs = date.split("-");
			return dateStrs[2] + "-" + dateStrs[0] + "-" + dateStrs[1];
		}
		return null;
	}

	public static int getYear(String time) {
		return Integer.parseInt(time.substring(0, 4));
	}

	public static int getMonth(String time) {
		return Integer.parseInt(time.substring(5, 7));
	}

	public static int getDay(String time) {
		return Integer.parseInt(time.substring(8, 10));
	}

	public static int getHours(String time) {
		return Integer.parseInt(time.substring(11, 13));
	}

	public static int getMinutes(String time) {
		return Integer.parseInt(time.substring(14, 16));
	}

	public static int getSeconds(String time) {
		return Integer.parseInt(time.substring(17));
	}
	
	public static int getYear() {
		GregorianCalendar g = new GregorianCalendar();
		return g.get(Calendar.YEAR);
	}

	public static int getMonth() {
		GregorianCalendar g = new GregorianCalendar();
		return g.get(Calendar.MONTH) + 1;
	}

	public static int getDay() {
		GregorianCalendar g = new GregorianCalendar();
		return g.get(Calendar.DAY_OF_MONTH);
	}

	public static int getHours() {
		GregorianCalendar g = new GregorianCalendar();
		return g.get(Calendar.HOUR_OF_DAY);
	}

	public static int getMinutes() {
		GregorianCalendar g = new GregorianCalendar();
		return g.get(Calendar.MINUTE);
	}

	public static int getSeconds() {
		GregorianCalendar g = new GregorianCalendar();
		return g.get(Calendar.SECOND);
	}
	
	public static void main(String[] args) {
		System.out.println("年:" + getYear());
		System.out.println("月:" + getMonth());
		System.out.println("日:" + getDay());
		System.out.println("时:" + getHours());
		System.out.println("分:" + getMinutes());
		System.out.println("秒:" + getSeconds());
	}

}