package lottery.domains.content.dao;

import lottery.domains.content.entity.Lottery;

import java.util.List;

public interface LotteryDao {
	
	List<Lottery> listAll();
	
}