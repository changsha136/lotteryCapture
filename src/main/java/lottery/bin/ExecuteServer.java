package lottery.bin;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ExecuteServer {
	private static final Logger mLogger = LoggerFactory.getLogger(ExecuteServer.class);
	// 每1小时点触发
	private static final String CRON = "0 0 0/1 * * *";
	// private static final String CRON = "0 0/1 * * * *";

	@Autowired
	Catalina mCatalina;

	@Scheduled(cron = CRON)
	public void execute() {
		mLogger.info("重启====>>>>>>>>>");
		mCatalina.executeOpenCai();
         //
		 // mCatalina.executeSSC();
         //
		 // mCatalina.execute11x5();
		 // mCatalina.executeK3();
	}

}
