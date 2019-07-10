package lottery.domains.content.biz;

import lottery.domains.content.entity.LotteryOpenCode;

/**
 * Created by Nick on 2017/2/25.
 */
public interface LotteryOpenCodeService {
    /**
     * 初始化开奖号码到redis中
     */
    void initLotteryOpenCode();

    /**
     * 查看某个彩种彩期是否已经抓取过了
     */
    boolean hasCaptured(String lotteryName, String expect);

    /**
     * 新增开奖号码，并放入到redis中
     */
    boolean add(LotteryOpenCode entity, boolean txffcAssistLast);


    LotteryOpenCode get(String lottery, String expect);
}
