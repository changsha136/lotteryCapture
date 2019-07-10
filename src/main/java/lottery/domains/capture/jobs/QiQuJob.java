package lottery.domains.capture.jobs;

import com.alibaba.fastjson.JSON;
import javautils.date.Moment;
import javautils.http.HttpClientUtil;
import lottery.domains.capture.sites.qqtj.QQTJBean;
import lottery.domains.capture.utils.CodeValidate;
import lottery.domains.capture.utils.ExpectValidate;
import lottery.domains.content.biz.LotteryOpenCodeService;
import lottery.domains.content.entity.LotteryOpenCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * 奇趣网腾讯分分彩
 */
@Component
public class QiQuJob {
    private static final String URL = "http://www.77tj.org/api/tencent/onlineim";
    private static final Logger logger = LoggerFactory.getLogger(QiQuJob.class);
    private static final String QQTXFFC   =   "qqtxffc";
    private static final String QQTX2FCD  =   "qqtx2fcd";
    private static final String QQTX2FCS  =   "qqtx2fcs";
    private static final String QQTX5FC   =   "qqtx5fc";
    private static final String QQTX3FC   =   "qqtx3fc";
    private static final String QQTX10FC  =   "qqtx10fc";
    @Autowired
    private LotteryOpenCodeService lotteryOpenCodeService;
    private static boolean isRuning = false;

    @Scheduled(cron = "0/3 * * * * *")
    // @PostConstruct
    public void execute() {
        synchronized (QiQuJob.class) {
            if (isRuning == true) {
                return;
            }
            isRuning = true;
        }
        try {
            logger.debug("开始抓取奇趣网腾讯分分彩开奖数据>>>>>>>>>>>>>>>>");
            long start = System.currentTimeMillis();
            start();
            long spend = System.currentTimeMillis() - start;
            logger.debug("完成抓取奇趣网腾讯分分彩开奖数据>>>>>>>>>>>>>>>>耗时{}", spend);
        } catch (Exception e) {
            logger.error("抓取奇趣网腾讯分分彩开奖数据出错", e);
        } finally {
            isRuning = false;
        }
    }

    private void start() {
        String result = getResult();
        handleData(result);
    }

    public String getResult() {
        String url = URL + "?_=" + System.currentTimeMillis();
        String charset = "UTF-8";
        String result = get(url, charset);
        return result;
    }

    private void handleData(String result) {
        if (StringUtils.isEmpty(result)) {
            return;
        }
        List<QQTJBean> openCodes = JSON.parseArray(result, QQTJBean.class);
        if (CollectionUtils.isEmpty(openCodes)) {
            logger.error("没有获取到奇趣网数据");
            return;
        }
        Collections.reverse(openCodes);
        // 处理数据
        for (QQTJBean bean : openCodes) {
            handleBean(bean);
        }
    }

    private boolean handleBean(QQTJBean bean) {
        boolean valid = checkData(bean);
        if (!valid) {
            return false;
        }
        Moment moment = new Moment().fromTime(bean.getOnlinetime());
        int hour = moment.get("hour");
        int minute = moment.get("minute");
        if (hour == 0 && minute == 0) {
            // 如果是0点0分，那么就是昨天的最后一期，即1440期
            moment = moment.add(-1, "minutes");
            hour = 24;
        }
        String date = moment.format("yyyyMMdd");
        int dayExpect = (hour * 60) + minute;
        String expectOne = date + "-" + (String.format("%04d", dayExpect));
        String code = convertCode(bean.getOnlinenumber());
        if (CodeValidate.validate(QQTXFFC, code) == false) {
            logger.error("奇趣网腾讯分分彩抓取号码" + code + "错误");
            return false;
        }
        if (ExpectValidate.validate(QQTXFFC, expectOne) == false) {
            logger.error("奇趣网腾讯分分彩抓取期数" + expectOne + "错误");
            return false;
        }
        // 如果人数无变化，那么把开奖号码状态改为无效撤单
        int status = 0;
        if (bean.getOnlinechange() == 0) {
            // 0：待开奖；1：已开奖；2：无效待撤单；3：无效已撤单
            status = 2;
        }
        LotteryOpenCode qqtxffcOpenCode = new LotteryOpenCode(QQTXFFC, expectOne, code, new Moment().toSimpleTime(), status, null, "QQTJ");
       // 一分钟的采集
        if (StringUtils.isNotEmpty(bean.getOnlinetime())) {
            qqtxffcOpenCode.setInterfaceTime(bean.getOnlinetime());
        } else {
            qqtxffcOpenCode.setInterfaceTime(new Moment().toSimpleTime());
        }
        boolean oneAddFlag = lotteryOpenCodeService.add(qqtxffcOpenCode, false);

        //两分钟（奇数）的采集
        if((dayExpect+1)%2==0){//
            String expectTwoOdd = date + "-" + (String.format("%03d", (dayExpect+1)/2));
            LotteryOpenCode qqtx2fcdOpenCode =new LotteryOpenCode(QQTX2FCD, expectTwoOdd, code, new Moment().toSimpleTime(), status, null, "QQTJ");
            if (StringUtils.isNotEmpty(bean.getOnlinetime())) {
                qqtx2fcdOpenCode.setInterfaceTime(bean.getOnlinetime());
            } else {
                qqtx2fcdOpenCode.setInterfaceTime(new Moment().toSimpleTime());
            }
            boolean twoOddAddFlag = lotteryOpenCodeService.add(qqtx2fcdOpenCode, false);
            if(twoOddAddFlag){
                logger.info("奇趣2分彩（单），第"+expectTwoOdd+"采集成功！");
            }
        }

        if(dayExpect%2==0){//
            String expectTwoEven = date + "-" + (String.format("%03d", dayExpect/2));
            LotteryOpenCode qqtx2fcsOpenCode =new LotteryOpenCode(QQTX2FCS, expectTwoEven, code, new Moment().toSimpleTime(), status, null, "QQTJ");
            if (StringUtils.isNotEmpty(bean.getOnlinetime())) {
                qqtx2fcsOpenCode.setInterfaceTime(bean.getOnlinetime());
            } else {
                qqtx2fcsOpenCode.setInterfaceTime(new Moment().toSimpleTime());
            }
            boolean twoEvenAddFlag = lotteryOpenCodeService.add(qqtx2fcsOpenCode, false);
            if(twoEvenAddFlag){
                logger.info("奇趣2分彩（双），第"+expectTwoEven+"采集成功！");
            }
        }

        //三分钟的采集
        if(dayExpect%3==0){
            String expectThree = date + "-" + (String.format("%03d", dayExpect/3));
            LotteryOpenCode qqtx5fcOpenCode =new LotteryOpenCode(QQTX3FC, expectThree, code, new Moment().toSimpleTime(), status, null, "QQTJ");
            if (StringUtils.isNotEmpty(bean.getOnlinetime())) {
                qqtx5fcOpenCode.setInterfaceTime(bean.getOnlinetime());
            } else {
                qqtx5fcOpenCode.setInterfaceTime(new Moment().toSimpleTime());
            }
            boolean fiveAddFlag = lotteryOpenCodeService.add(qqtx5fcOpenCode, false);
            if(fiveAddFlag){
                logger.info("奇趣三分彩，第"+expectThree+"采集成功！");
            }
        }

        //五分钟的采集
        if(dayExpect%5==0){
            String expectFive = date + "-" + (String.format("%03d", dayExpect/5));
            LotteryOpenCode qqtx5fcOpenCode =new LotteryOpenCode(QQTX5FC, expectFive, code, new Moment().toSimpleTime(), status, null, "QQTJ");
            if (StringUtils.isNotEmpty(bean.getOnlinetime())) {
                qqtx5fcOpenCode.setInterfaceTime(bean.getOnlinetime());
            } else {
                qqtx5fcOpenCode.setInterfaceTime(new Moment().toSimpleTime());
            }
            boolean fiveAddFlag = lotteryOpenCodeService.add(qqtx5fcOpenCode, false);
            if(fiveAddFlag){
                logger.info("奇趣五分彩，第"+expectFive+"采集成功！");
            }
        }

        //十分钟的采集
        if(dayExpect%10==0){
            String expectTen = date + "-" + (String.format("%03d", dayExpect/10));
            LotteryOpenCode qqtx10fcOpenCode =new LotteryOpenCode(QQTX10FC, expectTen, code, new Moment().toSimpleTime(), status, null, "QQTJ");
            if (StringUtils.isNotEmpty(bean.getOnlinetime())) {
                qqtx10fcOpenCode.setInterfaceTime(bean.getOnlinetime());
            } else {
                qqtx10fcOpenCode.setInterfaceTime(new Moment().toSimpleTime());
            }
            boolean tenAddFlag = lotteryOpenCodeService.add(qqtx10fcOpenCode, false);
            if(tenAddFlag){
                logger.info("奇趣五分彩，第"+expectTen+"采集成功！");
            }
        }
        return oneAddFlag;
    }

    public static String get(String urlAll, String charset) {
        try {
            String result = HttpClientUtil.get(urlAll, null, 5000);
            return result;
        } catch (Exception e) {
            logger.error("请求奇趣网出错", e);
            return null;
        }
    }


    private boolean checkData(QQTJBean bean) {
        if (bean == null) {
            logger.error("奇趣网数据非法，空数据");
            return false;
        }
        if (StringUtils.isEmpty(bean.getOnlinetime()) || bean.getOnlinenumber() <= 0) {
            logger.error("奇趣网数据非法:" + JSON.toJSONString(bean));
            return false;
        }

        // 最小要1000才能组成开奖号码，否则是会出错的
        if (bean.getOnlinenumber() < 1000) {
            logger.error("奇趣网数据非法:" + JSON.toJSONString(bean));
            return false;
        }

        return true;
    }

    /**
     * 转换号码
     * 万位：onlinenumber所有数值相加取尾数
     * 千位：onlinenumber倒数第4位
     * 百位：onlinenumber倒数第3位
     * 十位：onlinenumber倒数第2位
     * 个位：onlinenumber倒数第1位
     */
    private String convertCode(int onlinenumber) {
        String[] chars = (onlinenumber + "").split("");
        int sum = 0;
        for (String aChar : chars) {
            if (aChar != null && !"".equals(aChar)) {
                sum += (Integer.valueOf(aChar));
            }
        }

        // 万位
        String wan = sum + "";
        wan = wan.substring(wan.length() - 1);

        String qian = chars[chars.length - 4] + "";
        String bai = chars[chars.length - 3] + "";
        String shi = chars[chars.length - 2] + "";
        String ge = chars[chars.length - 1] + "";

        return wan + "," + qian + "," + bai + "," + shi + "," + ge;
    }

// 	public static void main(String[] args) {
// 		// System.out.println(convertCode2(214616782));
//
// 		Moment moment = new Moment().fromTime("2017-08-13 00:02:00");
// 		int hour = moment.get("hour");
// 		int minute = moment.get("minute");
//
// 		if (hour == 0 && minute == 0) {
// 			// 如果是0点0分，那么就是昨天的最后一期，即1440期
// 			moment = moment.add(-1, "minutes");
// 			hour = 24;
// 		}
// 		String date = moment.format("yyyyMMdd");
// 		// hour = moment.get("hour");
// 		// minute = moment.get("minute");
//
// 		int dayExpect = (hour * 60) + minute;
//
// 		String expect = date + "-" + (String.format("%04d", dayExpect));
//
// 		System.out.println(expect);
// 	}

    public static String convertCode2(int onlinenumber) {
        String[] chars = (onlinenumber + "").split("");
        int sum = 0;
        for (String aChar : chars) {
            if (aChar != null && !"".equals(aChar)) {
                sum += (Integer.valueOf(aChar));
            }
        }

        // 万位
        String wan = sum + "";
        wan = wan.substring(wan.length() - 1);

        String qian = chars[chars.length - 4] + "";
        String bai = chars[chars.length - 3] + "";
        String shi = chars[chars.length - 2] + "";
        String ge = chars[chars.length - 1] + "";

        return wan + "," + qian + "," + bai + "," + shi + "," + ge;
    }
}