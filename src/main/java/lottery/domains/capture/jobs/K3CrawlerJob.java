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
//  * 快三任务，废弃不再使用
//  *
//  * @author ROOT
//  *
//  */
// @Component
// public class K3CrawlerJob {
//
// 	private static final Logger logger = LoggerFactory.getLogger(K3CrawlerJob.class);
//
// 	@Autowired
// 	private JobConfig config;
//
// 	@Autowired
// 	private ShiShiCaiCrawler shishicai;
//
// 	@Autowired
// 	private Kai168H mKai168H;
//
// 	boolean isRunningAHK3 = true;
//
// 	/**
// 	 * 安徽快三
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.ahk3_8),
// 	// 		@Scheduled(cron = LotteryCrons.ahk3_9_21),
// 	// 		@Scheduled(cron = LotteryCrons.ahk3_22), })
// 	public void executeAHK3() {
// 		final String lotteryName = "ahk3";
// 		logger.debug(lotteryName + " execute");
// 		if (config.isExeK3 && isRunningAHK3) {
// 			try {
// 				isRunningAHK3 = false;
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
// 					logger.debug("安徽快三--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("安徽快三-->执行出错...", e);
// 			} finally {
// 				isRunningAHK3 = true;
// 			}
//
// 		}
// 	}
//
// 	boolean isRunningHBK3 = true;
//
// 	/**
// 	 * 湖北快3
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.hbk3_9_21),
// 	// 		@Scheduled(cron = LotteryCrons.hbk3_22) })
// 	public void executeHBK3() {
// 		final String lotteryName = "hbk3";
// 		logger.debug(lotteryName + " execute");
// 		if (config.isExeK3 && isRunningHBK3) {
// 			try {
// 				isRunningHBK3 = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
// 					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// 						@Override
// 						public boolean[] call() throws Exception {
// 							// boolean result1 = mKai168H.start(lotteryName);
// 							boolean result1 = false;
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
// 					logger.debug("湖北快3--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("湖北快3-->执行出错...", e);
// 			} finally {
// 				isRunningHBK3 = true;
// 			}
//
// 		}
// 	}
//
// 	boolean isRunningJLK3 = true;
//
// 	/**
// 	 * 吉林快3
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.jlk3_8),
// 	// 		@Scheduled(cron = LotteryCrons.jlk3_9_11),
// 	// 		@Scheduled(cron = LotteryCrons.jlk3_12),
// 	// 		@Scheduled(cron = LotteryCrons.jlk3_13_14),
// 	// 		@Scheduled(cron = LotteryCrons.jlk3_15),
// 	// 		@Scheduled(cron = LotteryCrons.jlk3_16_17),
// 	// 		@Scheduled(cron = LotteryCrons.jlk3_18),
// 	// 		@Scheduled(cron = LotteryCrons.jlk3_19_20),
// 	// 		@Scheduled(cron = LotteryCrons.jlk3_21) })
// 	public void executeJLK3() {
// 		final String lotteryName = "jlk3";
// 		logger.debug(lotteryName + " execute");
// 		if (config.isExeK3 && isRunningJLK3) {
// 			try {
// 				isRunningJLK3 = false;
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
// 					logger.debug("吉林快3--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("吉林快3-->执行出错...", e);
// 			} finally {
// 				isRunningJLK3 = true;
// 			}
// 		}
// 	}
//
// 	boolean isRunningJSK3 = true;
//
// 	/**
// 	 * 江苏快3
// 	 */
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.jsk3_8),
// 	// 		@Scheduled(cron = LotteryCrons.jsk3_9_15),
// 	// 		@Scheduled(cron = LotteryCrons.jsk3_16_20),
// 	// 		@Scheduled(cron = LotteryCrons.jsk3_21),
// 	// 		@Scheduled(cron = LotteryCrons.jsk3_22) })
// 	public void executeJSK3() {
// 		final String lotteryName = "jsk3";
// 		logger.debug(lotteryName + " execute");
// 		if (config.isExeK3 && isRunningJSK3) {
// 			try {
// 				isRunningJSK3 = false;
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
// 					logger.debug("江苏快3--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("江苏快3-->执行出错...", e);
// 			} finally {
// 				isRunningJSK3 = true;
// 			}
//
// 		}
// 	}
// }
