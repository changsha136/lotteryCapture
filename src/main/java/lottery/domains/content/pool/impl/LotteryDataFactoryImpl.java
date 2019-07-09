package lottery.domains.content.pool.impl;

import lottery.domains.content.biz.LotteryOpenCodeService;
import lottery.domains.content.dao.LotteryDao;
import lottery.domains.content.dao.LotteryOpenTimeDao;
import lottery.domains.content.entity.Lottery;
import lottery.domains.content.entity.LotteryOpenTime;
import lottery.domains.content.pool.LotteryDataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Component
public class LotteryDataFactoryImpl implements LotteryDataFactory, InitializingBean {

	private static final Logger logger = LoggerFactory.getLogger(LotteryDataFactoryImpl.class);
	
	@Override
	public void init() {
		logger.info("init LotteryDataFactory....start");
		initLottery();
		initLotteryOpenTime();
		initLotteryOpenCode();
		logger.info("init LotteryDataFactory....done");
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		this.init();
	}
	
	/**
	 * 初始化开奖时间信息
	 */
	@Autowired
	private LotteryOpenTimeDao lotteryOpenTimeDao;

	@Autowired
	private LotteryOpenCodeService lotteryOpenCodeService;

	private List<LotteryOpenTime> lotteryOpenTimeList = new LinkedList<>();
	
	@Override
	public void initLotteryOpenTime() {
		try {
			List<LotteryOpenTime> list = lotteryOpenTimeDao.listAll();
			if(lotteryOpenTimeList != null) {
				lotteryOpenTimeList.clear();
			}
			lotteryOpenTimeList.addAll(list);
			logger.info("初始化彩票开奖时间信息完成！");
		} catch (Exception e) {
			logger.error("初始化彩票开奖时间信息失败！");
		}
	}
	
	@Override
	public List<LotteryOpenTime> listLotteryOpenTime(String lottery) {
		List<LotteryOpenTime> list = new LinkedList<>();
		for (LotteryOpenTime tmpBean : lotteryOpenTimeList) {
			if(tmpBean.getLottery().equals(lottery)) {
				list.add(tmpBean);
			}
		}
		return list;
	}

	@Override
	public void initLotteryOpenCode() {
		lotteryOpenCodeService.initLotteryOpenCode();
	}

	@Autowired
	private LotteryDao lotteryDao;

	private Map<Integer, Lottery> lotteryMap = new LinkedHashMap<>();

	@Override
	public void initLottery() {
		try {
			List<Lottery> list = lotteryDao.listAll();
			Map<Integer, Lottery> tmpMap = new LinkedHashMap<>();
			for (Lottery lottery : list) {
				tmpMap.put(lottery.getId(), lottery);
			}
			lotteryMap = tmpMap;
			logger.info("初始化彩票信息完成！");
		} catch (Exception e) {
			logger.error("初始化彩票信息失败！");
		}
	}

	@Override
	public Lottery getLottery(int id) {
		if (lotteryMap.containsKey(id)) {
			return lotteryMap.get(id);
		}
		return null;
	}

	@Override
	public Lottery getLottery(String shortName) {
		Object[] keys = lotteryMap.keySet().toArray();
		for (Object o : keys) {
			Lottery lottery = lotteryMap.get(o);
			if (lottery.getShortName().equals(shortName)) {
				return lottery;
			}
		}
		return null;
	}

	@Override
	public List<Lottery> listLottery() {
		List<Lottery> list = new LinkedList<Lottery>();
		Object[] keys = lotteryMap.keySet().toArray();
		for (Object o : keys) {
			list.add(lotteryMap.get(o));
		}
		return list;
	}
}
