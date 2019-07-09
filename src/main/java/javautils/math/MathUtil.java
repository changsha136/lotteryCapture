package javautils.math;

import java.math.BigDecimal;

public class MathUtil {
	
	public static double decimalFormat(BigDecimal bd, int point) {
		return bd.setScale(point, BigDecimal.ROUND_HALF_DOWN).doubleValue();
	}
	
	public static double doubleFormat(double d, int point) {
		try {
			BigDecimal bd = new BigDecimal(d);
			return bd.setScale(point, BigDecimal.ROUND_HALF_DOWN).doubleValue();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return d;
	}
	
	public static float floatFormat(float f, int point) {
		try {
			BigDecimal bd = new BigDecimal(f);
			return bd.setScale(point, BigDecimal.ROUND_HALF_DOWN).floatValue();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return f;
	}
}
