package lottery.domains.capture.utils;

import javautils.StringUtil;

public class CodeValidate {
	public static boolean validate(String lottery, String code) {
		switch (lottery) {
			case "cqssc":
			case "ocqssc":
			case "xjssc":
			case "tjssc":
			case "tw5fc":
			case "bj5fc":
			case "jnd3d5fc":
			case "txffc":
			case "qqtxffc":
				return isSsc(code);
			case "sd11x5":
			case "gd11x5":
			case "jx11x5":
			case "ah11x5":
				return is11x5(code);
			case "jsk3":
			case "ahk3":
			case "jlk3":
			case "hbk3":
			case "shk3":
				return isK3(code);
			case "fc3d":
			case "pl3":
				return is3d(code);
			case "bjkl8":
				return isBjkl8(code);
			case "bjpk10":
			case "objpk10":
				return isBjpk10(code);
			default:
				return true;
		}
	}

	public static boolean isSsc(String s) {
		// 是否为空
		if (!StringUtil.isNotNull(s))
			return false;
		String[] codes = s.split(",");
		// 数字长度
		if (codes.length != 5)
			return false;
		for (String tmpS : codes) {
			// 是否是数字
			if (!StringUtil.isInteger(tmpS))
				return false;
			// 数字位数对不对
			if (tmpS.length() != 1)
				return false;
			int tmpC = Integer.parseInt(tmpS);
			// 数字的有效范围
			if (!(tmpC >= 0 && tmpC <= 9))
				return false;
		}
		return true;
	}

	public static boolean is11x5(String s) {
		// 是否为空
		if (!StringUtil.isNotNull(s))
			return false;
		String[] codes = s.split(",");
		// 数字长度
		if (codes.length != 5)
			return false;
		for (String tmpS : codes) {
			// 是否是数字
			if (!StringUtil.isInteger(tmpS))
				return false;
			// 数字位数对不对
			if (tmpS.length() != 2)
				return false;
			int tmpC = Integer.parseInt(tmpS);
			// 数字的有效范围
			if (!(tmpC >= 1 && tmpC <= 11))
				return false;
		}
		return true;
	}

	public static boolean isK3(String s) {
		// 是否为空
		if (!StringUtil.isNotNull(s))
			return false;
		String[] codes = s.split(",");
		// 数字长度
		if (codes.length != 3)
			return false;
		for (String tmpS : codes) {
			// 是否是数字
			if (!StringUtil.isInteger(tmpS))
				return false;
			// 数字位数对不对
			if (tmpS.length() != 1)
				return false;
			int tmpC = Integer.parseInt(tmpS);
			// 数字的有效范围
			if (!(tmpC >= 1 && tmpC <= 6))
				return false;
		}
		return true;
	}

	public static boolean is3d(String s) {
		// 是否为空
		if (!StringUtil.isNotNull(s))
			return false;
		String[] codes = s.split(",");
		// 数字长度
		if (codes.length != 3)
			return false;
		for (String tmpS : codes) {
			// 是否是数字
			if (!StringUtil.isInteger(tmpS))
				return false;
			// 数字位数对不对
			if (tmpS.length() != 1)
				return false;
			int tmpC = Integer.parseInt(tmpS);
			// 数字的有效范围
			if (!(tmpC >= 0 && tmpC <= 9))
				return false;
		}
		return true;
	}

	public static boolean isBjkl8(String s) {
		// 是否为空
		if (!StringUtil.isNotNull(s))
			return false;
		String[] codes = s.split(",");
		// 数字长度
		if (codes.length != 20)
			return false;
		for (String tmpS : codes) {
			// 是否是数字
			if (!StringUtil.isInteger(tmpS))
				return false;
			// 数字位数对不对
			if (tmpS.length() != 2)
				return false;
			int tmpC = Integer.parseInt(tmpS);
			// 数字的有效范围
			if (!(tmpC >= 1 && tmpC <= 80))
				return false;
		}
		return true;
	}

	public static boolean isBjpk10(String s) {
		// 是否为空
		if (!StringUtil.isNotNull(s))
			return false;
		String[] codes = s.split(",");
		// 数字长度
		if (codes.length != 10)
			return false;
		for (String tmpS : codes) {
			// 是否是数字
			if (!StringUtil.isInteger(tmpS))
				return false;
			// 数字位数对不对
			if (tmpS.length() != 2)
				return false;
			int tmpC = Integer.parseInt(tmpS);
			// 数字的有效范围
			if (!(tmpC >= 1 && tmpC <= 10))
				return false;
		}
		return true;
	}

	public static boolean isKy481(String s) {
		// 是否为空
		if (!StringUtil.isNotNull(s))
			return false;
		String[] codes = s.split(",");
		// 数字长度
		if (codes.length != 4)
			return false;
		for (String tmpS : codes) {
			// 是否是数字
			if (!StringUtil.isInteger(tmpS))
				return false;
			// 数字位数对不对
			if (tmpS.length() != 1)
				return false;
			int tmpC = Integer.parseInt(tmpS);
			// 数字的有效范围
			if (!(tmpC >= 1 && tmpC <= 8))
				return false;
		}
		return true;
	}

}