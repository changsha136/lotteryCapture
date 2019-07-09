// package lottery.domains.capture.jobs;
//
// import lottery.domains.capture.sites.kj168.Kai168H;
// import lottery.domains.capture.sites.shishicai.ShiShiCaiCrawler;
// import lottery.domains.capture.sites.tjflcpw.TjflcpwCrawler;
// import lottery.domains.capture.sites.xjflcp.XjflcpCrawler;
// import lottery.domains.capture.utils.LotteryCrons;
// import lottery.domains.content.global.JobConfig;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.scheduling.annotation.Schedules;
// import org.springframework.stereotype.Component;
//
// import java.util.concurrent.Callable;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.Future;
//
// /**
//  * 重庆时时彩，废弃不再使用
//  *
//  * @author root
//  */
// @Component
// public class SSCCrawlerJob {
//
// 	private static final Logger logger = LoggerFactory.getLogger(SSCCrawlerJob.class);
//
// 	@Autowired
// 	private JobConfig config;
//
// 	@Autowired
// 	private Kai168H mKai168H;
//
// 	@Autowired
// 	private ShiShiCaiCrawler shishicai;
//
// 	public boolean isRunningCQSSC = true;
// 	public boolean isRunningXJSSC = true;
// 	public boolean isRunningJXSSC = true;
// 	public boolean isRunningTJSSC = true;
//
// 	/**
// 	 * 重庆时时彩
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.cqssc_5),
// 	// 		@Scheduled(cron = LotteryCrons.cqssc_10) })
// 	public void executeCQSSC() {
// 		final String lotteryName = "cqssc";
// 		if (config.isExeSsc && isRunningCQSSC) {
// 			isRunningCQSSC = false;
// 			try {
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
// 					logger.debug(lotteryName + ">>> execute");
// 					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// 						@Override
// 						public boolean[] call() throws Exception {
// 							boolean result1 = mKai168H.start(lotteryName);
// 							boolean result2 = shishicai.start(lotteryName);
// 							boolean[] arr = new boolean[] { result1, result2 };
// 							return arr;
// 						}
// 					};
// 					Future<?> submit = newFixedThreadPool.submit(aCallable);
// 					newFixedThreadPool.shutdown();
// 					boolean[] arr = (boolean[]) submit.get();
// 					if (arr[0] || arr[1]){
// 						isRunningCQSSC = true;
// 						break;
// 					}
// 					Thread.sleep(10 * 1000);
// 				}
// 			} catch (Exception e) {
// 				logger.error("时时彩数据抓取异常...", e);
// 			} finally {
// 				isRunningCQSSC = true;
// 			}
// 		}
// 	}
//
// 	// ==========================
// 	/**
// 	 * 江西时时彩
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.jxssc_9),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_10),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_11),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_12),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_13),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_14),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_15),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_16),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_17),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_18),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_19),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_20),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_21),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_22),
// 	// 		@Scheduled(cron = LotteryCrons.jxssc_23) })
// 	public void executeJXSSC() {
// //		final String lotteryName = "jxssc";
// //		boolean isRunningJXSSC = true;
// //		if (config.isExeSsc && isRunningJXSSC) {
// //			isRunningJXSSC = false;
// //			try {
// //				while (true) {
// //					ExecutorService newFixedThreadPool = Executors
// //							.newFixedThreadPool(4);
// //					logger.debug(lotteryName + ">>>>> execute");
// //					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// //						@Override
// //						public boolean[] call() throws Exception {
// //							boolean result1 = mKai168H.start(lotteryName);
// //							boolean result2 = shishicai.start(lotteryName);
// //							boolean[] arr = new boolean[] { result1, result2 };
// //							return arr;
// //						}
// //					};
// //					Future<?> submit = newFixedThreadPool.submit(aCallable);
// //					newFixedThreadPool.shutdown();
// //					boolean[] arr = (boolean[]) submit.get();
// //					if (arr[0] || arr[1])
// //						break;
// //					Thread.sleep(10 * 1000);
// //					logger.debug("执行完成...");
// //				}
// //			} catch (Exception e) {
// //				logger.error("执行出错...", e);
// //			} finally {
// //				isRunningJXSSC = true;
// //			}
// //
// //		}
// 	}
//
// 	// ===================天津=====
// 	@Autowired
// 	private TjflcpwCrawler tjflcpw;
//
// 	/**
// 	 * 天津时时彩
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.tjssc) })
// 	public void executeTJSSC() {
// 		final String lotteryName = "tjssc";
// 		if (config.isExeSsc && isRunningTJSSC) {
// 			isRunningTJSSC = false;
// 			try {
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
// 					logger.debug(lotteryName + " execute");
// 					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// 						@Override
// 						public boolean[] call() throws Exception {
// 							boolean result1 = mKai168H.start(lotteryName);
// 							// shishicai.cn 没有这个获取
// 							// boolean result2 =
// 							// shishicai.start(lotteryName);
// 							boolean result2 = false;
// 							boolean[] arr = new boolean[] { result1, result2 };
// 							return arr;
// 						}
// 					};
// 					Future<?> submit = newFixedThreadPool.submit(aCallable);
// 					newFixedThreadPool.shutdown();
// 					boolean[] arr = (boolean[]) submit.get();
// 					if (arr[0] || arr[1])
// 						break;
// 					Thread.sleep(10 * 1000);
// 					logger.debug("天津scc 执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("天津ssc 执行出错...", e);
// 			} finally {
// 				isRunningTJSSC = true;
// 			}
//
// 		}
// 	}
//
// 	// ----------------------新疆-------
// 	@Autowired
// 	private XjflcpCrawler xjflcp;
//
// 	/**
// 	 * 新疆时时彩
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.xjssc_0_1),
// 	// 		@Scheduled(cron = LotteryCrons.xjssc_2),
// 	// 		@Scheduled(cron = LotteryCrons.xjssc_10),
// 	// 		@Scheduled(cron = LotteryCrons.xjssc_11_23) })
// 	public void executeXJSSC() {
// 		final String lotteryName = "xjssc";
// 		if (config.isExeSsc && isRunningXJSSC) {
// 			try {
// 				isRunningXJSSC = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
// 					logger.debug(lotteryName + ">>>>> execute");
// 					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// 						@Override
// 						public boolean[] call() throws Exception {
// 							boolean result1 = mKai168H.start(lotteryName);
// 							// boolean result2 = xjflcp.start(lotteryName);
// 							boolean result2 = false;
// 							boolean[] arr = new boolean[] { result1, result2 };
// 							return arr;
// 						}
// 					};
// 					Future<?> submit = newFixedThreadPool.submit(aCallable);
// 					newFixedThreadPool.shutdown();
// 					boolean[] arr = (boolean[]) submit.get();
// 					if (arr[0] || arr[1])
// 						break;
// 					Thread.sleep(10 * 1000);
// 					logger.debug("新疆 ssc 执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("新疆ssc 执行出错...", e);
// 			} finally {
// 				isRunningXJSSC = true;
// 			}
// 		}
// 	}
// }