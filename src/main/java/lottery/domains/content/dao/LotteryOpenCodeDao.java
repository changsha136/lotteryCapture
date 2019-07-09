package lottery.domains.content.dao;

import lottery.domains.content.entity.LotteryOpenCode;

import java.util.List;

public interface LotteryOpenCodeDao {

	LotteryOpenCode get(String lottery, String expect);
	
	boolean add(LotteryOpenCode entity);

	List<LotteryOpenCode> getLatest(String lotteryName, int count);
}