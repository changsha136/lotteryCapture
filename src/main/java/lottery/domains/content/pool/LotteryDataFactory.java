package lottery.domains.content.pool;

import java.util.List;

import lottery.domains.content.entity.Lottery;
import lottery.domains.content.entity.LotteryOpenTime;

public interface LotteryDataFactory {

	/**
	 * 初始化所有配置
	 */
	void init();
	
	/**
	 * 初始化彩票开奖时间信息
	 */
	void initLotteryOpenTime();
	
	/**
	 * 列出彩票开奖时间信息
	 * @param lottery
	 * @return
	 */
	List<LotteryOpenTime> listLotteryOpenTime(String lottery);

	/**
	 * 初始化开奖号码到redis中
	 */
	void initLotteryOpenCode();

	/**
	 * 初始化彩票信息
	 */
	void initLottery();

	/**
	 * 获取彩票信息
	 * @param id
	 * @return
	 */
	Lottery getLottery(int id);

	/**
	 * 获取彩票信息
	 * @param shortName
	 * @return
	 */
	Lottery getLottery(String shortName);

	/**
	 * 列出所有彩票
	 * @return
	 */
	List<Lottery> listLottery();
}