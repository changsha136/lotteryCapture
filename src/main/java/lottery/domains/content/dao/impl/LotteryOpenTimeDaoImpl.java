package lottery.domains.content.dao.impl;

import java.util.List;

import javautils.jdbc.hibernate.HibernateSuperDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import lottery.domains.content.dao.LotteryOpenTimeDao;
import lottery.domains.content.entity.LotteryOpenTime;

@Repository
public class LotteryOpenTimeDaoImpl implements LotteryOpenTimeDao {

	private final String tab = LotteryOpenTime.class.getSimpleName();
	
	@Autowired
	private HibernateSuperDao<LotteryOpenTime> superDao;
	
	@Override
	@Transactional(readOnly = true)
	public List<LotteryOpenTime> listAll() {
		String hql = "from " + tab + " order by expect asc";
		return superDao.list(hql);
	}

}