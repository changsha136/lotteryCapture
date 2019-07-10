package lottery.domains.capture.jobs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import javautils.date.Moment;
import javautils.http.HttpClientUtil;
import lottery.domains.capture.sites.qq03.TxffcBean;
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

import java.net.SocketTimeoutException;
import java.util.*;

@Component
public class TxffcJob {
	private static final Logger logger = LoggerFactory.getLogger(TxffcJob.class);
	private static boolean isRuning = false;
	private static final String URL = "http://www.qq03.com/api/external/list-tecent-online/"  ;
	// 彩票code列表
	private static final Map<String, String> LOTTERIES = new HashMap<>();
	static {
		// key：对方code，value：我方code
		LOTTERIES.put("txffc", "txffc"); // 腾讯分分彩
		LOTTERIES.put("tx5fc", "tx5fc"); // 腾讯5分彩
		LOTTERIES.put("tx5pk10", "tx5pk10"); // 腾讯PK10
	}

	@Autowired
	private LotteryOpenCodeService lotteryOpenCodeService;


//    @Scheduled(cron = "0/10 * * * * *") // 注意频率，每次间隔大于1秒
	@Scheduled(cron = "6,10,15,20,25,30,35 * * * * *")
	// @PostConstruct
	public void execute() {
		synchronized (TxffcJob.class) {
			if (isRuning == true) {
				return;
			}
			isRuning = true;
		}

		try {
			logger.debug("开始抓取腾讯系列开奖数据>>>>>>>>>>>>>>>>");
			long start = System.currentTimeMillis();
			start();
			long spend = System.currentTimeMillis() - start;
			logger.debug("完成抓取腾讯系列开奖数据>>>>>>>>>>>>>>>>耗时{}", spend);
		} catch (Exception e) {
			logger.error("抓取腾讯分分彩开奖数据出错", e);
		} finally {
			isRuning = false;
		}
	}

	private void start() {
		for (String lottery : LOTTERIES.keySet()) {
			try {
				String realName = LOTTERIES.get(lottery);
				String result = getResult(lottery, 10);
				if("fail".endsWith(result)){
					handleData(realName, result);
				}
			} catch (Exception e) {
				logger.error("抓取腾讯分分彩"+lottery+"开奖数据出错", e);
			}
		}
	}

	public String getResult(String name, int num) {
		String result = null;
		try {
			result = HttpClientUtil.get(URL + name, null, 5000);
		} catch (Exception e) {
		}
		return result;
	}
	public static void main(String[] args) {
		String result="{'error':0,'code':null,'message':'请求成功','data':{'list':[{'issue':'201905120920','openTime':'2019-05-12 15:20:00','openCode':'0,1,3,9,3','onlineNum':null,'algorithm':null,'hashValue':null},{'issue':'201905120919','openTime':'2019-05-12 15:19:00','openCode':'6,5,9,9,1','onlineNum':null,'algorithm':null,'hashValue':null},{'issue':'201905120918','openTime':'2019-05-12 15:18:00','openCode':'9,2,3,6,8','onlineNum':null,'algorithm':null,'hashValue':null},{'issue':'201905120917','openTime':'2019-05-12 15:17:00','openCode':'3,0,8,0,6','onlineNum':null,'algorithm':null,'hashValue':null},{'issue':'201905120916','openTime':'2019-05-12 15:16:00','openCode':'5,6,9,6,6','onlineNum':null,'algorithm':null,'hashValue':null},{'issue':'201905120915','openTime':'2019-05-12 15:15:00','openCode':'1,5,3,0,5','onlineNum':null,'algorithm':null,'hashValue':null},{'issue':'201905120914','openTime':'2019-05-12 15:14:00','openCode':'7,8,2,3,7','onlineNum':null,'algorithm':null,'hashValue':null},{'issue':'201905120913','openTime':'2019-05-12 15:13:00','openCode':'2,9,8,9,0','onlineNum':null,'algorithm':null,'hashValue':null},{'issue':'201905120912','openTime':'2019-05-12 15:12:00','openCode':'8,2,5,1,5','onlineNum':null,'algorithm':null,'hashValue':null},{'issue':'201905120911','openTime':'2019-05-12 15:11:00','openCode':'0,4,3,6,4','onlineNum':null,'algorithm':null,'hashValue':null}]}}";
		JSONObject jsonObject = JSON.parseObject(result);
		String data2 =jsonObject.getString("data");
		JSONObject list = JSON.parseObject(data2);
		String openCodes =list.getString("list");
		JSONArray openCodesJson = JSON.parseArray(openCodes);
     	List<TxffcBean> openCodes2 = JSON.parseArray(openCodesJson.toJSONString(), TxffcBean.class);
		System.out.println(openCodes2);
	}

	private void handleData(String realName, String result) {
		if (StringUtils.isEmpty(result)) {
			return;
		}
		JSONObject jsonObject = JSON.parseObject(result);
		String data2 =jsonObject.getString("data");
		JSONObject list = JSON.parseObject(data2);
		String openCodesTemp =list.getString("list");
		JSONArray jsonArr = JSON.parseArray(openCodesTemp);
//		JSONArray jsonArr = jsonObject.getJSONArray("data");
		List<TxffcBean> openCodes = JSON.parseArray(jsonArr.toJSONString(), TxffcBean.class);
		if (CollectionUtils.isEmpty(openCodes)) {
			return;
		}
		// 处理数据
		for (TxffcBean openCode : openCodes) {
			handleBean(realName, openCode);
		}
	}

	private boolean handleBean(String realName, TxffcBean openCode) {
		openCode.setIssue(formartExpect(openCode.getIssue(),realName));
		LotteryOpenCode dbData = lotteryOpenCodeService.get(realName, openCode.getIssue());
		if (dbData != null) {
			if (!dbData.getCode().equals(openCode.getOpenCode())) {
				logger.error("抓取时遇到错误：抓取{}期开奖号码{}与数据库已有开奖号码{}不符", openCode.getIssue(), openCode.getOpenCode(), dbData.getCode());
				return false;
			}
			return true;
		}
		// 如果本期和上奖开奖号码相同，那么把开奖号码状态改为无效撤单
		LotteryOpenCode lotteryOpenCode = new LotteryOpenCode();
		lotteryOpenCode.setInterfaceTime(openCode.getOpenTime());
		lotteryOpenCode.setLottery(realName);
		lotteryOpenCode.setTime(new Moment().toSimpleTime());
		lotteryOpenCode.setOpenStatus(0);
		lotteryOpenCode.setRemarks("www.qq03.com");
		switch (realName) {
			// 腾讯分分彩
			case "txffc":
				lotteryOpenCode.setCode(openCode.getOpenCode());
				lotteryOpenCode.setExpect(openCode.getIssue());
				break;
			case "tx5fc":
				lotteryOpenCode.setCode(openCode.getOpenCode());
				lotteryOpenCode.setExpect(openCode.getIssue());
				break;
			case "tx5pk10":
				lotteryOpenCode.setCode(openCode.getOpenCode());
				lotteryOpenCode.setExpect(openCode.getIssue());
				break;
			default:
				break;
		}

		if (StringUtils.isEmpty(lotteryOpenCode.getCode()) || StringUtils.isEmpty(lotteryOpenCode.getExpect())) {
			return false;
		}
		if (CodeValidate.validate(realName, lotteryOpenCode.getCode()) == false) {
			logger.error("腾讯分分彩" + realName + "抓取号码" + lotteryOpenCode.getCode() + "错误");
			return false;
		}
		if (ExpectValidate.validate(realName, lotteryOpenCode.getExpect()) == false) {
			logger.error("腾讯分分彩" + realName + "抓取期数" + lotteryOpenCode.getExpect() + "错误");
			return false;
		}
		boolean added = lotteryOpenCodeService.add(lotteryOpenCode, true);
		if (added) {
            logger.info("官网成功抓取腾讯分分彩{}期开奖号码{}", openCode.getIssue(), openCode.getOpenCode());
            String tempExpect;
			tempExpect=lotteryOpenCode.getExpect();
			// 腾讯龙虎斗
			if ("txffc".equals(realName)) {
				LotteryOpenCode txffcOpenCode = new LotteryOpenCode("txffc", lotteryOpenCode.getExpect(), lotteryOpenCode.getCode(), lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(), null, lotteryOpenCode.getRemarks());
				txffcOpenCode.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
				lotteryOpenCodeService.add(txffcOpenCode, false);
				//截取后四位
                int expectInt = Integer.parseInt(lotteryOpenCode.getExpect().substring(tempExpect.length()-4, tempExpect.length()));
                //2分彩单号
                if((expectInt+1)%2==0){
                    String  twoOddExpect =tempExpect.substring(0,8)+"-"+ String.format("%03d", (expectInt+1)/2);
                    LotteryOpenCode tx2fcdOpenCode = new LotteryOpenCode("tx2fcd", twoOddExpect, lotteryOpenCode.getCode(), lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(), null, lotteryOpenCode.getRemarks());
                    tx2fcdOpenCode.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
                    Boolean tx2fcdAddFlag=lotteryOpenCodeService.add(tx2fcdOpenCode, false);
                    if(tx2fcdAddFlag){
                        logger.info("腾讯2分彩（奇），第"+twoOddExpect+"抓取成功");
                    }
                }
                if(expectInt%2==0){
                    String  twoEvenExpect =tempExpect.substring(0,8)+"-"+ String.format("%03d", expectInt/2);
                    LotteryOpenCode tx2fcsOpenCode = new LotteryOpenCode("tx2fcs", twoEvenExpect, lotteryOpenCode.getCode(), lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(), null, lotteryOpenCode.getRemarks());
                    tx2fcsOpenCode.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
                    Boolean tx2fcsAddFlag=lotteryOpenCodeService.add(tx2fcsOpenCode, false);
                    if(tx2fcsAddFlag){
                        logger.info("腾讯2分彩（偶），第"+twoEvenExpect+"抓取成功");
                    }
                }
				if(expectInt%3==0){
					String  threeExpect =tempExpect.substring(0,8)+"-"+ String.format("%03d", expectInt/3);
					LotteryOpenCode tx3fcOpenCode = new LotteryOpenCode("tx3fc", threeExpect, lotteryOpenCode.getCode(), lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(), null, lotteryOpenCode.getRemarks());
					tx3fcOpenCode.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
					Boolean tx3fcAddFlag=lotteryOpenCodeService.add(tx3fcOpenCode, false);
					if(tx3fcAddFlag){
						logger.info("腾讯3分彩，第"+threeExpect+"抓取成功");
					}
				}
			}
			if ("tx5fc".equals(realName)) {
				LotteryOpenCode tx5fcOpenCode = new LotteryOpenCode("tx5fc", lotteryOpenCode.getExpect(), lotteryOpenCode.getCode(), lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(), null, lotteryOpenCode.getRemarks());
                tx5fcOpenCode.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
				lotteryOpenCodeService.add(tx5fcOpenCode, false);

				//截取后四位
				int expectInt = Integer.parseInt(lotteryOpenCode.getExpect().substring(tempExpect.length()-3, tempExpect.length()));
				//10分彩采集
				if(expectInt%2==0){
					String  tenExpect =tempExpect.substring(0,8)+"-"+ String.format("%03d", expectInt/2);
					LotteryOpenCode tx10fcOpenCode = new LotteryOpenCode("tx10fc", tenExpect, lotteryOpenCode.getCode(), lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(), null, lotteryOpenCode.getRemarks());
					tx10fcOpenCode.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
					Boolean tx10fcAddFlag=lotteryOpenCodeService.add(tx10fcOpenCode, false);
					if(tx10fcAddFlag){
						logger.info("腾讯10分彩，第"+tenExpect+"抓取成功");
					}
				}
			}
			if ("tx5pk10".equals(realName)) {
				LotteryOpenCode tx3fcOpenCode = new LotteryOpenCode("tx5pk10", lotteryOpenCode.getExpect(), lotteryOpenCode.getCode(), lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(), null, lotteryOpenCode.getRemarks());
				tx3fcOpenCode.setInterfaceTime(lotteryOpenCode.getInterfaceTime());
				lotteryOpenCodeService.add(tx3fcOpenCode, false);
			}
		}
		return added;
	}

	private static String getExpectByTime(String time) {
		Moment moment = new Moment().fromTime(time);
		int hour = moment.get("hour");
		int minute = moment.get("minute");
		if (hour == 0 && minute == 0) {
			// 如果是0点0分，那么就是昨天的最后一期，即1440期
			moment = moment.add(-1, "minutes");
			hour = 24;
		}
		String date = moment.format("yyyyMMdd");
		int dayExpect = (hour * 60) + minute;
		String expect = date + "-" + (String.format("%04d", dayExpect));
		return expect;
	}

	private String formartExpect(String expect,String realName ){
		String date = expect.substring(0,8);
		String num;
		if("txffc".endsWith(realName)){
			  num = String.format("%04d", Integer.valueOf(expect.substring(8)));
		}else{
			  num = String.format("%03d", Integer.valueOf(expect.substring(8)));
		}
		return date + "-" +num;
	}

	public static String get(String urlAll) {
		try {
			String result = HttpClientUtil.get(urlAll, null, 5000);
			return result;
		} catch (SocketTimeoutException e) {
			logger.error("请求腾讯采集超时", e);
			return null;
		} catch (Exception e) {
			logger.error("请求腾讯分分彩出错", e);
			return null;
		}
	}
}