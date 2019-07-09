package lottery.utils;

import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Random;

import lottery.utils.math.MathUtil;
import lottery.utils.regex.RegexUtil;

public class StringUtil {

	/**
	 * 判断字符串是否为空
	 * @param s
	 * @return
	 */
	public static boolean isNotNull(String s) {
		if(s == null) return false;
		if(s.trim().length() == 0) return false;
		return true;
	}
	
	/**
	 * 判断字符串能否转成int
	 * @param s
	 * @return
	 */
	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	/**
	 * 判断字符串能否转成double
	 * @param s
	 * @return
	 */
	public static boolean isDouble(String s) {
		try {
			Double.parseDouble(s);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	/**
	 * 判断字符串能否转成float
	 * @param s
	 * @return
	 */
	public static boolean isFloat(String s) {
		try {
			Float.parseFloat(s);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	public static String fromInputStream(InputStream inputStream) {
		try {
			StringBuffer sb = new StringBuffer();
			byte[] bytes = new byte[1024];
			int len;
			while ((len = inputStream.read(bytes)) != -1) {
				sb.append(new String(bytes, 0, len));
			}
			inputStream.close();
			return sb.toString();
		} catch (Exception e) {}
		return null;
	}
	
	/**
	 * 创建随机字符串
	 * @param config
	 * @return
	 */
	public static String createString(int length) {
		StringBuffer sb = new StringBuffer();
		char ch[] = { 
				'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 
				'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 
				'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 
				'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1'};
		Random random = new Random();
		if (length > 0) {
			int index = 0;
			char[] temp = new char[length];
			int num = random.nextInt();
			for (int i = 0; i < length % 5; i++) {
				temp[index++] = ch[num & 63];
				num >>= 6;
			}
			for (int i = 0; i < length / 5; i++) {
				num = random.nextInt();
				for (int j = 0; j < 5; j++) {
					temp[index++] = ch[num & 63];
					num >>= 6;
				}
			}
			sb.append(new String(temp, 0, length));
		}
		return sb.toString();
	}
	
	/**
	 * 创建随机字符串
	 * Example like {6, "-", 9}
	 * @param config
	 * @return
	 */
	public static String createString(Object[] config) {
		StringBuffer sb = new StringBuffer();
		for (Object key : config) {
			if(key instanceof Integer) {
				sb.append(createString((Integer)key));
			}
			if(key instanceof String) {
				sb.append(key);
			}
		}
		return sb.toString();
	}
	
	/**
	 * 创建随机数字
	 * @param config
	 * @return
	 */
	public static String createNumberString(int length) {
		StringBuffer sb = new StringBuffer();
		char ch[] = { 
				'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '9', '8', '7', '6', '5', '4', 
				'3', '2', '1', '0', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '9', '8', 
				'7', '6', '5', '4', '3', '2', '1', '0', '0', '1', '2', '3', '4', '5', '6', '7', 
				'8', '9', '9', '8', '7', '6', '5', '4', '3', '2', '1', '0', '0', '1', '2', '3'};
		Random random = new Random();
		if (length > 0) {
			int index = 0;
			char[] temp = new char[length];
			int num = random.nextInt();
			for (int i = 0; i < length % 5; i++) {
				temp[index++] = ch[num & 63];
				num >>= 6;
			}
			for (int i = 0; i < length / 5; i++) {
				num = random.nextInt();
				for (int j = 0; j < 5; j++) {
					temp[index++] = ch[num & 63];
					num >>= 6;
				}
			}
			sb.append(new String(temp, 0, length));
		}
		return sb.toString();
	}
	
	/**
	 * 转换文件大小
	 * @param bytes
	 * @return
	 */
	public static String transDataLong(long b) {
		StringBuffer sb = new StringBuffer();
		long KB = 1024;
		long MB = KB * 1024;
		long GB = MB * 1024;
		long TB = GB * 1024;
		if(b >= TB) {
			sb.append(MathUtil.doubleFormat((double)b/TB, 2) + "TB");
		} else if(b >= GB) {
			sb.append(MathUtil.doubleFormat((double)b/GB, 2) + "GB");
		} else if(b >= MB) {
			sb.append(MathUtil.doubleFormat((double)b/MB, 2) + "MB");
		} else if(b >= KB) {
			sb.append(MathUtil.doubleFormat((double)b/KB, 2) + "KB");
		} else {
			sb.append(b + "B");
		}
		return sb.toString();
	}
	
	
	/**
	 * 判断是否是int类型字符串
	 * @param str
	 * @return
	 */
	public static boolean isIntegerString(String str) {
		boolean flag = false;
		if(RegexUtil.isMatcher(str, RegexUtil.integer_regex)) {
			flag = true;
		}
		return flag;
	}
	
	public static boolean isDoubleString(String str) {
		boolean flag = false;
		if(RegexUtil.isMatcher(str, RegexUtil.rational_numbers_regex)) {
			flag = true;
		}
		return flag;
	}
	
	public static boolean isFloatString(String str) {
		boolean flag = false;
		if(RegexUtil.isMatcher(str, RegexUtil.rational_numbers_regex)) {
			flag = true;
		}
		return flag;
	}
	
	/**
	 * 判断字符串是否是日期yyyy-MM-dd格式
	 * @param s
	 * @return
	 */
	public static boolean isDateString(String str) {
		try {
			if(str.length() != 10) {
				return false;
			}
			new SimpleDateFormat("yyyy-MM-dd").parse(str);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	public static String markWithSymbol(Object obj, String symbol) {
		return symbol + obj.toString() + symbol;
	}
	
	//123456789
	public static Object[] split(String s) {
		char[] carr = s.toCharArray();
		Object[] arr = new Object[carr.length];
		for (int i = 0; i < carr.length; i++) {
			arr[i] = carr[i];
		}
		return arr;
	}
	
	public static String substring(String s, String start, String end, boolean isInSub) {
		int idx = s.indexOf(start);
		int edx = s.indexOf(end);
		idx = idx == -1 ? 0 : idx + (isInSub ? 0 : start.length());
		edx = edx == -1 ? s.length() : edx + (isInSub ? end.length() : 0);
		return s.substring(idx, edx);
	}
	
	public static String doubleFormat(double d) {
		DecimalFormat decimalformat = new  DecimalFormat("#0.00"); 
		return decimalformat.format(d);
	}
	
	public static void main(String[] args) {
		
	}
	
}
