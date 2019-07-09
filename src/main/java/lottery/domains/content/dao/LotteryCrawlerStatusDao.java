package lottery.domains.content.dao;

import lottery.domains.content.entity.LotteryCrawlerStatus;

public interface LotteryCrawlerStatusDao {
	
	LotteryCrawlerStatus get(String lottery);
	
	boolean update(String shortName, String lastExpect, String lastUpdate);
	
}