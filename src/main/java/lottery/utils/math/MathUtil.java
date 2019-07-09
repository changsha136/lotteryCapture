package lottery.utils.math;

import java.math.BigDecimal;

public class MathUtil {
	
	public static double doubleFormat(double d, int point) {
		try {
			BigDecimal bd = new BigDecimal(d);
			return bd.setScale(point, BigDecimal.ROUND_HALF_DOWN).doubleValue();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return d;
	}
	
}
