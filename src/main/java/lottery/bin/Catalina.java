package lottery.bin;

import java.util.concurrent.ExecutorService;

// import lottery.domains.capture.jobs.K3CrawlerJob;
// import lottery.domains.capture.jobs.OpenCaiJob;
// import lottery.domains.capture.jobs.OthersCrawlerJob;
// import lottery.domains.capture.jobs.SSCCrawlerJob;
// import lottery.domains.capture.jobs.X511CrawlerJob;
import lottery.domains.content.global.JobConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Catalina {
	// int corePoolSize = 5;
	// int maxPoolSize = 10;
	// long keepAliveTime = 5000;
	//
	// ExecutorService threadPoolExecutor = new ThreadPoolExecutor(corePoolSize,
	// maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS,
	// new LinkedBlockingQueue<Runnable>());
	ExecutorService newFixedThreadPool;

	@Autowired
	private JobConfig config;

	// @Autowired
	// private OpenCaiJob mOpenCaiJob;

	/**
	 * OpenCai
	 */
	public void executeOpenCai() {
		// config.isExeOpenCai = true;
		// config.isExeOthers = true;
//		mOthersCrawlerJob.executeHN481();
	}

	// @Autowired
	// private SSCCrawlerJob mSscCrawlerJob;

	/**
	 * 时时彩
	 */
	public void executeSSC() {
		// config.isExeSsc = true;
		// mSscCrawlerJob.executeCQSSC();
		// //mSscCrawlerJob.executeJXSSC();
		// mSscCrawlerJob.executeTJSSC();
		// mSscCrawlerJob.executeXJSSC();
	}

	// @Autowired
	// private X511CrawlerJob mX511CrawlerJob;

	/**
	 * 11选5
	 */
	public void execute11x5() {
		// config.isExe11x5 = true;
		// mX511CrawlerJob.executeAH11x5();
		// mX511CrawlerJob.executeGD11x5();
		// mX511CrawlerJob.executeJX11x5();
		// mX511CrawlerJob.executeSD11x5();
		// mX511CrawlerJob.executeSH11x5();
	}

	// @Autowired
	// private K3CrawlerJob mK3CrawlerJob;

	/**
	 * 快三
	 */
	public void executeK3() {
		// config.isExeK3 = true;
		// mK3CrawlerJob.executeAHK3();
		// mK3CrawlerJob.executeHBK3();
		// mK3CrawlerJob.executeJLK3();
		// mK3CrawlerJob.executeJSK3();
	}

	// @Autowired
	// private OthersCrawlerJob mOthersCrawlerJob;

	/**
	 * 北京快乐8、北京pk0、福彩3d、排列三
	 */
	public void executeOthers() {
		// config.isExeOthers = true;
		// mOthersCrawlerJob.executeBJKL8();
		// mOthersCrawlerJob.executeBJPK8();
		// mOthersCrawlerJob.executeFC3D();
		// mOthersCrawlerJob.executePL3();
//		mOthersCrawlerJob.executeHN481();
	}
}