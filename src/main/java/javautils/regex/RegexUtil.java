package javautils.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javautils.StringUtil;

public final class RegexUtil {
	
	private RegexUtil() {
		
	}

	/**
	 * 匹配图象
	 * 格式: /相对路径/文件名.后缀 (后缀为gif,dmp,png,jpg)
	 * 匹配 : /forum/head_icon/admini2005111_ff.gif 或 admini2005111.dmp
	 * 不匹配: c:/admins4512.gif
	 */
	public static final String image_regex = "^(/{0,1}\\w){1,}\\.(gif|dmp|png|jpg|jpeg)$|^\\w{1,}\\.(gif|dmp|png|jpg|jpeg)$";

	/**
	 * 匹配email地址
	 * 匹配 : foo@bar.com 或 foobar@foobar.com.au
	 * 不匹配: foo@bar 或 $$$@bar.com
	 */
	public static final String email_regex = "(?:\\w[-._\\w]*\\w@\\w[-._\\w]*\\w\\.\\w{2,3}$)";

	/**
	 * 匹配匹配并提取url
	 * 匹配 : http://www.suncer.com 或news://www
	 * 提取(MatchResult matchResult=matcher.getMatch()): matchResult.group(0)=
	 * http://www.suncer.com:8080/index.html?login=true matchResult.group(1) =
	 * http matchResult.group(2) = www.suncer.com matchResult.group(3) = :8080
	 * matchResult.group(4) = /index.html?login=true
	 * 不匹配: c:\window
	 */
	public static final String url_regex = "(\\w+)://([^/:]+)(:\\d*)?([^\\s]*)";

	/**
	 * 匹配并提取http
	 * 匹配 : http://www.suncer.com:8080/index.html?login=true
	 * 提取(MatchResult matchResult=matcher.getMatch()): matchResult.group(0)=
	 * http://www.suncer.com:8080/index.html?login=true matchResult.group(1) =
	 * http matchResult.group(2) = www.suncer.com matchResult.group(3) = :8080
	 * matchResult.group(4) = /index.html?login=true
	 * 不匹配: news://www
	 */
	public static final String http_regex = "(http|https|ftp)://([^/:]+)(:\\d*)?([^\\s]*)";

	/**
	 * 匹配日期
	 * 范围:1900--2099
	 * 匹配 : 2005-04-04
	 * 不匹配: 01-01-01
	 */
	public static final String date_regex = "^((((19){1}|(20){1})d{2})|d{2})[-\\s]{1}[01]{1}d{1}[-\\s]{1}[0-3]{1}d{1}$";// 匹配日期

	/**
	 * 匹配电话
	 * 匹配 : 0371-123456 或 (0371)1234567 或 (0371)12345678 或 010-123456 或
	 * 010-12345678 或 12345678912
	 * 不匹配: 1111-134355 或 0123456789
	 */
	public static final String phone_regex = "^(?:0[0-9]{2,3}[-\\s]{1}|\\(0[0-9]{2,4}\\))[0-9]{6,8}$|^[1-9]{1}[0-9]{5,7}$|^[1-9]{1}[0-9]{10}$";

	/**
	 * 匹配身份证
	 * 匹配 : 0123456789123
	 * 不匹配: 0123456
	 */
	public static final String ID_card_regex = "^\\d{10}|\\d{13}|\\d{15}|\\d{18}$";

	/**
	 * 匹配邮编代码
	 * 匹配 : 012345
	 * 不匹配: 0123456
	 */
	public static final String ZIP_regex = "^[0-9]{6}$";// 匹配邮编代码

	/**
	 * 不包括特殊字符的匹配 (字符串中不包括符号 数学次方号^ 单引号' 双引号" 分号; 逗号, 帽号: 数学减号- 右尖括号> 左尖括号< 反斜杠\
	 * 即空格,制表符,回车符等 )
	 * 格式为: x 或 一个一上的字符
	 * 匹配 : 012345
	 * 不匹配: 0123456
	 */
	public static final String non_special_char_regex = "^[^'\"\\;,:-<>\\s].+$";// 匹配邮编代码

	/**
	 * 匹配非负整数（正整数 + 0)
	 */
	public static final String non_negative_integers_regex = "^\\d+$";

	/**
	 * 匹配不包括零的非负整数（正整数 > 0)
	 */
	public static final String non_zero_negative_integers_regex = "^[1-9]+\\d*$";

	/**
	 * 匹配正整数
	 */
	public static final String positive_integer_regex = "^[0-9]*[1-9][0-9]*$";

	/**
	 * 匹配非正整数（负整数 + 0）
	 */
	public static final String non_positive_integers_regex = "^((-\\d+)|(0+))$";

	/**
	 * 匹配负整数
	 */
	public static final String negative_integers_regex = "^-[0-9]*[1-9][0-9]*$";

	/**
	 * 匹配整数
	 */
	public static final String integer_regex = "^-?\\d+$";

	/**
	 * 匹配非负浮点数（正浮点数 + 0）
	 */
	public static final String non_negative_rational_numbers_regex = "^\\d+(\\.\\d+)?$";

	/**
	 * 匹配正浮点数
	 */
	public static final String positive_rational_numbers_regex = "^(([0-9]+\\.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*\\.[0-9]+)|([0-9]*[1-9][0-9]*))$";

	/**
	 * 匹配非正浮点数（负浮点数 + 0）
	 */
	public static final String non_positive_rational_numbers_regex = "^((-\\d+(\\.\\d+)?)|(0+(\\.0+)?))$";

	/**
	 * 匹配负浮点数
	 */
	public static final String negative_rational_numbers_regex = "^(-(([0-9]+\\.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*\\.[0-9]+)|([0-9]*[1-9][0-9]*)))$";

	/**
	 * 匹配浮点数
	 */
	public static final String rational_numbers_regex = "^(-?\\d+)(\\.\\d+)?$";

	/**
	 * 匹配由26个英文字母组成的字符串
	 */
	public static final String letter_regex = "^[A-Za-z]+$";

	/**
	 * 匹配由26个英文字母的大写组成的字符串
	 */
	public static final String upward_letter_regex = "^[A-Z]+$";

	/**
	 * 匹配由26个英文字母的小写组成的字符串
	 */
	public static final String lower_letter_regex = "^[a-z]+$";

	/**
	 * 匹配由数字和26个英文字母组成的字符串
	 */
	public static final String letter_number_regex = "^[A-Za-z0-9]+$";

	/**
	 * 匹配由数字、26个英文字母或者下划线组成的字符串
	 */
	public static final String letter_number_underline_regex = "^\\w+$";
	
	public static boolean isMatcher(String s, String regex){
		boolean flag = false;
		try {
			if(StringUtil.isNotNull(s)) {
				Pattern pattern = Pattern.compile(regex);
				Matcher matcher = pattern.matcher(s);
				flag = matcher.matches();
			}
			
		} catch (Exception e) {
			System.out.println("匹配失败！");
		}
		return flag;
	}
}