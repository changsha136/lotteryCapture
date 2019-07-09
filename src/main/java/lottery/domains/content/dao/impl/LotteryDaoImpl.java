package lottery.domains.content.dao.impl;

import javautils.jdbc.hibernate.HibernateSuperDao;
import lottery.domains.content.dao.LotteryDao;
import lottery.domains.content.entity.Lottery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class LotteryDaoImpl implements LotteryDao {

	private final String tab = Lottery.class.getSimpleName();
	
	@Autowired
	private HibernateSuperDao<Lottery> superDao;
	
	@Override
	public List<Lottery> listAll() {
		String hql = "from " + tab + " order by sort";
		return superDao.list(hql);
	}
	
}