package lottery.domains.capture.utils;

public class LotteryCrons {
	
	/**
	 * 格式: [秒] [分] [小时] [日] [月] [周] [年] 
	 */
	private LotteryCrons() {}
	
	public static final String TEST = "0/10 * * * * *";
	// 重庆时时彩5分钟档
	public static final String cqssc_5 = "36 0/5 0-1,22-23 * * *";
	// 重庆时时彩10分钟档
	public static final String cqssc_10 = "6 0/10 10-21 * * *";
	
	// 江西时时彩时时彩9点
	public static final String jxssc_9 = "0 10,20,30,40,50 9 * * *";
	// 江西时时彩时时彩10点
	public static final String jxssc_10 = "0 0,10,21,31,41,51 10 * * *";
	// 江西时时彩11点
	public static final String jxssc_11 = "0 1,11,21,32,42,52 11 * * *";
	// 江西时时彩12点
	public static final String jxssc_12 = "0 2,12,22,32,43,53 12 * * *";
	// 江西时时彩13点
	public static final String jxssc_13 = "0 3,13,23,33,43,54 13 * * *";
	// 江西时时彩14点
	public static final String jxssc_14 = "0 4,14,24,34,44,55 14 * * *";
	// 江西时时彩15点
	public static final String jxssc_15 = "0 5,15,25,35,45,55 15 * * *";
	// 江西时时彩16点
	public static final String jxssc_16 = "0 6,16,26,36,46,56 16 * * *";
	// 江西时时彩17点
	public static final String jxssc_17 = "0 6,17,27,37,47,57 17 * * *";
	// 江西时时彩18点
	public static final String jxssc_18 = "0 7,18,28,38,48,58 18 * * *";
	// 江西时时彩19点
	public static final String jxssc_19 = "0 8,18,29,39,49,59 19 * * *";
	// 江西时时彩20点
	public static final String jxssc_20 = "0 9,19,29,40,50 20 * * *";
	// 江西时时彩21点
	public static final String jxssc_21 = "0 0,10,20,30,40,51 21 * * *";
	// 江西时时彩22点
	public static final String jxssc_22 = "0 1,11,21,31,41,51 22 * * *";
	// 江西时时彩23点
	public static final String jxssc_23 = "0 2,12 23 * * *";
	
	// 新疆时时彩10:10~10:50
	public static final String xjssc_10 = "0 10/10 10 * * *";
	// 新疆时时彩11~23点
	public static final String xjssc_11_23 = "0 0/10 11-23 * * *";
	// 新疆时时彩0~1点
	public static final String xjssc_0_1 = "0 0/10 0-1 * * *";
	// 新疆时时彩2:00点
	public static final String xjssc_2 = "0 0 2 * * *";
	
	// 天津时时彩9~22点
	public static final String tjssc = "0 9/10 9-22 * * *";
	
	// 广东11选5 09:10~09:50
	public static final String gd11x5_9 = "0 9/10 10 * * *";
	// 广东11选5 10~22点
	public static final String gd11x5_10_22 = "0 0/10 10-22 * * *";
	// 广东11选5 23点00
	public static final String gd11x5_23 = "0 0 23 * * *";
	
	
	// 江西11选5 9:10~10:50
	public static final String jx11x5_9 = "0 10/10 10 * * *";
	// 江西11选5 10~21点
	public static final String jx11x5_10_21 = "0 0/10 10-21 * * *";
	// 江西11选5 22点
	public static final String jx11x5_22 = "0 0 22 * * *";
	
	// 重庆11选5 9~22点
	public static final String cq11x5_9_22 = "0 0/10 9-22 * * *";
	// 重庆11选5 23点
	public static final String cq11x5_23 = "0 0 23 * * *";
	
	// 安徽11选5 8点
	public static final String ah11x5_8 = "0 40,50 8 * * *";
	// 安徽11选5 9~21点
	public static final String ah11x5_9_21 = "0 0/10 9-21 * * *";
	// 安徽11选5 22点
	public static final String ah11x5_22 = "0 0 22 * * *";
	
	// 上海11选5 9~23点
	public static final String sh11x5_9_23 = "0 0/10 9-23 * * *";
	
	// 山东11选5 9~23点
	public static final String sd11x5_9_21 = "0 5/10 9-21 * * *";
	
	// 江苏快3 8点
	public static final String jsk3_8 = "0 40,50 8 * * *";
	// 江苏快3 9~15点
	public static final String jsk3_9_15 = "0 0/10 9-15 * * *";
	// 江苏快3 16~20点
	public static final String jsk3_16_20 = "0 1/10 16-20 * * *";
	// 江苏快3 21点
	public static final String jsk3_21 = "0 1,11,21,32,42,52 21 * * *";
	// 江苏快3 22点
	public static final String jsk3_22 = "0 2,12 22 * * *";
	
	// 安徽快3 8点
	public static final String ahk3_8 = "0 50 8 * * *";
	// 安徽快3 9~21点
	public static final String ahk3_9_21 = "0 0/10 9-21 * * *";
	// 安徽快3 22点
	public static final String ahk3_22 = "0 0 22 * * *";
	
	// 湖北快3 9~21点
	public static final String hbk3_9_21 = "0 0/10 9-21 * * *";
	// 湖北快3 22点
	public static final String hbk3_22 = "0 0 22 * * *";
	
	// 吉林快3 8点
	public static final String jlk3_8 = "0 29,39,49 8 * * *";
	// 吉林快3 9~11点
	public static final String jlk3_9_11 = "0 0/10 9-11 * * *";
	// 吉林快3 12点
	public static final String jlk3_12 = "0 0,10,21,31,41,51 12 * * *";
	// 吉林快3 13~14点
	public static final String jlk3_13_14 = "0 1/10 13-14 * * *";
	// 吉林快3 15点
	public static final String jlk3_15 = "0 1,11,21,32,42,52 15 * * *";
	// 吉林快3 16~17点
	public static final String jlk3_16_17 = "0 2/10 16-17 * * *";
	// 吉林快3 18点
	public static final String jlk3_18 = "0 2,12,23,33,43,53 18 * * *";
	// 吉林快3 19~20点
	public static final String jlk3_19_20 = "0 3/10 19-20 * * *";
	// 吉林快3 21点
	public static final String jlk3_21 = "0 3,14,24,34 21 * * *";
	
	// 北京快乐8
	public static final String bjkl8 = "0 0/5 9-23 * * *";
	
	// 北京PK拾
	public static final String bjpk10 = "0 7/5 9-23 * * *";
	
	// 福彩3d
	public static final String fc3d = "0 30 20 * * *";
	
	// 排列三
	public static final String pl3 = "0 50 20 * * *";
	
	// 河南481 9点
	public static final String hn481_9 = "0 10,20,30,40,50 9 * * *";
	// 河南481 10~21点
	public static final String hn481_10_21 = "0 0/10 10-21 * * *";
	// 河南481 22点
	public static final String hn481_22 = "0 0 22 * * *";
	
}