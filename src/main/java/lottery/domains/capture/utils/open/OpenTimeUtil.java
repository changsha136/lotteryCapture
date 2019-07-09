package lottery.domains.capture.utils.open;

public interface OpenTimeUtil {
	
	String getCurrExpect(String lotteryName, String currTime);
	
	String getNextExpect(String lotteryName, String lastExpect);
	
}