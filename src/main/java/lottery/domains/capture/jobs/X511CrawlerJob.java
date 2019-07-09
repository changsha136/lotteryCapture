// package lottery.domains.capture.jobs;
//
// import lottery.domains.capture.sites.kj168.Kai168H;
// import lottery.domains.capture.sites.shishicai.ShiShiCaiCrawler;
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
//  * 11选5任务，废弃不再使用
//  *
//  * @author root
//  */
// @Component
// public class X511CrawlerJob {
//
// 	private static final Logger logger = LoggerFactory.getLogger(X511CrawlerJob.class);
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
// 	boolean isRunningAH11X5 = true;
//
// 	/**
// 	 * 安徽11选5
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.ah11x5_8),
// 	// 		@Scheduled(cron = LotteryCrons.ah11x5_9_21),
// 	// 		@Scheduled(cron = LotteryCrons.ah11x5_22) })
// 	public void executeAH11x5() {
// 		final String lotteryName = "ah11x5";
// 		logger.debug(lotteryName + " execute");
// 		if (config.isExe11x5 && isRunningAH11X5) {
// 			try {
// 				isRunningAH11X5 = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
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
// 					if (arr[0] || arr[1]) {
// 						break;
// 					}
// 					Thread.sleep(10 * 1000);
// 					logger.debug("安徽11选5--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("安徽11选5-->执行出错...", e);
// 			} finally {
// 				isRunningAH11X5 = true;
// 			}
// 		}
// 	}
//
// 	boolean isRunningHG11X5 = true;
//
// 	/**
// 	 * 广东11选5
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.gd11x5_9),
// 	// 		@Scheduled(cron = LotteryCrons.gd11x5_10_22),
// 	// 		@Scheduled(cron = LotteryCrons.gd11x5_23) })
// 	public void executeGD11x5() {
// 		final String lotteryName = "gd11x5";
// 		logger.debug(lotteryName + " execute");
// 		if (config.isExe11x5 && isRunningHG11X5) {
// 			try {
// 				isRunningHG11X5 = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
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
// 					if (arr[0] || arr[1]) {
// 						break;
// 					}
// 					Thread.sleep(10 * 1000);
// 					logger.debug("广东11选5--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("广东11选5-->执行出错...", e);
// 			} finally {
// 				isRunningHG11X5 = true;
// 			}
// 		}
// 	}
//
// 	boolean isRunningSH11X5 = true;
//
// 	/**
// 	 * 上海11选5
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.sh11x5_9_23) })
// 	public void executeSH11x5() {
// 		final String lotteryName = "sh11x5";
// 		logger.debug(lotteryName + " execute");
// 		if (config.isExe11x5 && isRunningSH11X5) {
// 			try {
// 				isRunningSH11X5 = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
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
// 					if (arr[0] || arr[1]) {
// 						break;
// 					}
// 					Thread.sleep(10 * 1000);
// 					logger.debug("上海11选5--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("上海11选5-->执行出错...", e);
// 			} finally {
// 				isRunningSH11X5 = true;
// 			}
// 		}
// 	}
//
// 	boolean isRunningSD11X5 = true;
//
// 	/**
// 	 * 山东11选5/十一夺运
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.sd11x5_9_21) })
// 	public void executeSD11x5() {
// 		final String lotteryName = "sd11x5";
// 		logger.debug(lotteryName + " execute");
// 		if (config.isExe11x5 && isRunningSD11X5) {
// 			try {
// 				isRunningSD11X5 = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
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
// 					if (arr[0] || arr[1]) {
// 						break;
// 					}
// 					Thread.sleep(10 * 1000);
// 					logger.debug("山东11选5---> 执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("山东11选5-->执行出错...", e);
// 			} finally {
// 				isRunningSD11X5 = true;
// 			}
// 		}
// 	}
//
// 	boolean isRunningJX11X5 = true;
//
// 	/**
// 	 * 江西11选5
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.jx11x5_9),
// 	// 		@Scheduled(cron = LotteryCrons.jx11x5_10_21),
// 	// 		@Scheduled(cron = LotteryCrons.jx11x5_22) })
// 	public void executeJX11x5() {
// 		final String lotteryName = "jx11x5";
//
// 		logger.debug(lotteryName + " execute");
// 		if (config.isExe11x5 && isRunningJX11X5) {
// 			try {
// 				isRunningJX11X5 = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
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
// 					if (arr[0] || arr[1]) {
// 						break;
// 					}
// 					Thread.sleep(10 * 1000);
// 					logger.debug("江西11选5---> 执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("江西11选5-->执行出错...", e);
// 			} finally {
// 				isRunningJX11X5 = true;
// 			}
// 		}
// 	}
//
// }