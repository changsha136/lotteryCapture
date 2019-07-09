// package lottery.domains.capture.jobs;
//
// import lottery.domains.capture.sites.bwlc.BwlcCrawler;
// import lottery.domains.capture.sites.bwlc.BwlcCrawlerFC3D;
// import lottery.domains.capture.sites.hn481.Hn481Crawler;
// import lottery.domains.capture.sites.kj168.Kai168BJ;
// import lottery.domains.capture.sites.kj168.Kai168L;
// import lottery.domains.capture.sites.lecai.LecaiCrawlerBJC;
// import lottery.domains.capture.sites.lecai.LecaiCrawlerPl3;
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
//  * 其他单个菜种的地址，废弃不再使用
//  *
//  * @author ROOT
//  *
//  */
// @Component
// public class OthersCrawlerJob {
//
// 	private static final Logger logger = LoggerFactory.getLogger(OthersCrawlerJob.class);
//
// 	@Autowired
// 	private JobConfig config;
//
// 	@Autowired
// 	private Kai168BJ mKai168bj;
//
// 	@Autowired
// 	private Kai168L mKai168L;
//
// 	@Autowired
// 	private BwlcCrawler mBwlcCrawlerJob;
//
// 	@Autowired
// 	private BwlcCrawlerFC3D mBwlcCrawlerFC3D;
//
// 	@Autowired
// 	private LecaiCrawlerBJC mLecaiCrawlerBJC;
//
// 	@Autowired
// 	private LecaiCrawlerPl3 mLecaiCrawlerPl3;
//
// 	@Autowired
// 	private Hn481Crawler hn481Job1;
//
// 	boolean isRunningBJKL8 = true;
//
// 	/**
// 	 * 北京快乐8
// 	 */
// 	// @Scheduled(cron = LotteryCrons.bjkl8)
// 	public void executeBJKL8() {
// 		final String lotteryName = "bjkl8";
// 		if (config.isExeOthers && isRunningBJKL8) {
// 			isRunningBJKL8 = false;
// 			try {
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
// 					logger.debug(lotteryName + " execute");
// 					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// 						@Override
// 						public boolean[] call() throws Exception {
// 							boolean result1 = mKai168bj.start(lotteryName);
// 							boolean result2 = mBwlcCrawlerJob
// 									.start(lotteryName);
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
// 					logger.debug("北京快乐8--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("北京快乐8-->执行出错...", e);
// 			} finally {
// 				isRunningBJKL8 = true;
// 			}
//
// 		}
// 	}
//
// 	boolean isRunningBJPK10 = true;
//
// 	/**
// 	 * 北京PK拾
// 	 */
// 	// @Scheduled(cron = LotteryCrons.bjpk10)
// 	public void executeBJPK8() {
// 		final String lotteryName = "bjpk10";
// 		if (config.isExeOthers && isRunningBJPK10) {
// 			try {
// 				isRunningBJPK10 = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
// 					logger.debug(lotteryName + " execute");
// 					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// 						@Override
// 						public boolean[] call() throws Exception {
// 							boolean result1 = mKai168bj.start(lotteryName);
// 							boolean result2 = mBwlcCrawlerJob
// 									.start(lotteryName);
// //							boolean result3 = mLecaiCrawlerBJC
// //									.start(lotteryName);
// 							boolean result3 = false;
// 							boolean[] arr = new boolean[] { result1, result2,
// 									result3 };
// 							return arr;
// 						}
// 					};
// 					Future<?> submit = newFixedThreadPool.submit(aCallable);
// 					newFixedThreadPool.shutdown();
// 					boolean[] arr = (boolean[]) submit.get();
// 					if (arr[0] || arr[1] || arr[2]) {
// 						break;
// 					}
// 					Thread.sleep(10 * 1000);
// 					logger.debug("北京PK拾--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("北京PK拾-->执行出错...", e);
// 			} finally {
// 				isRunningBJPK10 = true;
// 			}
//
// 		}
// 	}
//
// 	boolean isRunningFC3D = true;
//
// 	/**
// 	 * 福彩3d
// 	 */
// 	// @Scheduled(cron = LotteryCrons.fc3d)
// 	public void executeFC3D() {
// 		final String lotteryName = "fc3d";
// 		if (config.isExeOthers && isRunningFC3D) {
// 			isRunningFC3D = false;
// 			try {
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
// 					logger.debug(lotteryName + " execute");
// 					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// 						@Override
// 						public boolean[] call() throws Exception {
// 							boolean result1 = mKai168L.start(lotteryName);
// 							boolean result2 = mBwlcCrawlerFC3D
// 									.start(lotteryName);
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
// 					logger.debug("福彩3d--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("福彩3d-->执行出错...", e);
// 			} finally {
// 				isRunningFC3D = true;
// 			}
//
// 		}
// 	}
//
// 	boolean isRunningPL3 = true;
//
// 	/**
// 	 * 排列三
// 	 */
// 	// @Scheduled(cron = LotteryCrons.pl3)
// 	public void executePL3() {
// 		final String lotteryName = "pl3";
// 		if (config.isExeOthers && isRunningPL3) {
// 			try {
// 				isRunningPL3 = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
// 					logger.debug(lotteryName + " execute");
// 					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// 						@Override
// 						public boolean[] call() throws Exception {
// 							boolean result1 = mKai168L.start(lotteryName);
// 							boolean result2 = mLecaiCrawlerPl3
// 									.start(lotteryName);
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
// 					logger.debug("排列三--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("排列三-->执行出错...", e);
// 			} finally {
// 				isRunningPL3 = true;
// 			}
//
// 		}
// 	}
//
// 	boolean isRunningHN481 = true;
//
// 	/**
// 	 * 河南481
// 	 */
//
// 	// @Schedules({ @Scheduled(cron = LotteryCrons.hn481_9),
// 	// 		@Scheduled(cron = LotteryCrons.hn481_10_21),
// 	// 		@Scheduled(cron = LotteryCrons.hn481_22) })
// 	public void executeHN481() {
// 		final String lotteryName = "hn481";
// 		if (config.isExeOthers && isRunningHN481) {
// 			try {
// 				isRunningHN481 = false;
// 				while (true) {
// 					ExecutorService newFixedThreadPool = Executors
// 							.newFixedThreadPool(4);
// 					logger.debug(lotteryName + " execute");
// 					Callable<boolean[]> aCallable = new Callable<boolean[]>() {
// 						@Override
// 						public boolean[] call() throws Exception {
// 							boolean result1 = hn481Job1.start(lotteryName);
// 							boolean result2 = false;
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
// 					logger.debug("河南481--->执行完成...");
// 				}
// 			} catch (Exception e) {
// 				logger.error("河南481-->执行出错...", e);
// 			} finally {
// 				isRunningHN481 = true;
// 			}
//
// 		}
// 	}
// }
