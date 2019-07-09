package lottery.domains.content.dao.impl;

import javautils.jdbc.hibernate.HibernateSuperDao;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import lottery.domains.content.dao.LotteryOpenCodeDao;
import lottery.domains.content.entity.LotteryOpenCode;

import java.util.List;

@Repository
public class LotteryOpenCodeDaoImpl implements LotteryOpenCodeDao {

	private final String tab = LotteryOpenCode.class.getSimpleName();
	
	@Autowired
	private HibernateSuperDao<LotteryOpenCode> superDao;

	@Override
	public LotteryOpenCode get(String lottery, String expect) {
		String hql = "from " + tab + " where lottery = ?0 and expect = ?1 and userId is null";
		Object[] values = {lottery, expect};
		List<LotteryOpenCode> list = superDao.list(hql, values);
		if (CollectionUtils.isNotEmpty(list)) {
			return list.get(0);
		}
		else {
			return null;
		}

		// return (LotteryOpenCode)
	}

	@Override
	public boolean add(LotteryOpenCode entity) {
		// LotteryOpenCode bean = get(entity.getLottery(), entity.getExpect());
		// if(bean == null) {
		// 	return superDao.save(entity);
		// }
		// return false;
		return superDao.save(entity);
	}

	@Override
	public List<LotteryOpenCode> getLatest(String lotteryName, int count) {
		String hql = "from " + tab + " where lottery = ?0 and userId is null order by expect desc";
		Object[] values = new Object[]{lotteryName};
		return superDao.list(hql, values, 0, count);
	}
}