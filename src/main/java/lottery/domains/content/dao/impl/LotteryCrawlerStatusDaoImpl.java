package lottery.domains.content.dao.impl;

import javautils.jdbc.hibernate.HibernateSuperDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import lottery.domains.content.dao.LotteryCrawlerStatusDao;
import lottery.domains.content.entity.LotteryCrawlerStatus;

@Repository
public class LotteryCrawlerStatusDaoImpl implements LotteryCrawlerStatusDao {

	private final String tab = LotteryCrawlerStatus.class.getSimpleName();
	
	@Autowired
	private HibernateSuperDao<LotteryCrawlerStatus> superDao;
	
	@Override
	public LotteryCrawlerStatus get(String shortName) {
		String hql = "from " + tab + " where shortName = ?0";
		Object[] values = {shortName};
		return (LotteryCrawlerStatus) superDao.unique(hql, values);
	}

	@Override
	public boolean update(String shortName, String lastExpect, String lastUpdate) {
		String hql = "update " + tab + " set lastExpect = ?1, lastUpdate = ?2 where shortName = ?0";
		Object[] values = {shortName, lastExpect, lastUpdate};
		return superDao.update(hql, values);
	}

}